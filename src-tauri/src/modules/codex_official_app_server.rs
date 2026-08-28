use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::{json, Value as JsonValue};
use uuid::Uuid;

#[cfg(windows)]
use std::os::windows::process::CommandExt;

#[cfg(target_os = "macos")]
const CODEX_APP_SERVER_MACOS_EXECUTABLES: &[&str] = &[
    "/Applications/ChatGPT.app/Contents/Resources/codex",
    "/Applications/Codex.app/Contents/Resources/codex",
];
const CODEX_APP_SERVER_EXECUTABLE_ENV: &str = "CODEX_APP_SERVER_EXECUTABLE";
const APP_SERVER_RESPONSE_TIMEOUT: Duration = Duration::from_secs(20);
const OFFICIAL_LOGIN_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const OFFICIAL_HOSTED_AUTH_ENDPOINT: &str = "https://chatgpt.com/codex/desktop-auth";
const OFFICIAL_CLIENT_IDENTITY_FILE: &str = "codex-official-client-identity.txt";

lazy_static::lazy_static! {
    static ref OFFICIAL_CLIENT_STABLE_ID: std::sync::Mutex<Option<String>> =
        std::sync::Mutex::new(None);
}

struct OfficialLoginSession {
    login_id: String,
    auth_url: String,
    completion: std::sync::Arc<(
        std::sync::Mutex<Option<Result<(), String>>>,
        std::sync::Condvar,
    )>,
    // 持有 stdin，保持 app-server JSON-RPC 会话存活直到回调完成。
    stdin: ChildStdin,
    child: Child,
}

/// 将 app-server 返回的内层授权地址包装成官方桌面使用的 hosted login 地址。
///
/// 官方桌面会在交给 `chatgpt.com/codex/desktop-auth` 前补充客户端身份字段；
/// 这里保持 PKCE/state/redirect_uri 等动态参数原样，只规范化这些固定字段。
fn hosted_auth_url(auth_url: &str) -> String {
    let enriched_auth_url = enrich_official_authorize_url(auth_url);
    format!(
        "{}?authorize_url={}&codex_streamlined_login=true&no_universal_links=1",
        OFFICIAL_HOSTED_AUTH_ENDPOINT,
        urlencoding::encode(&enriched_auth_url)
    )
}

fn enrich_official_authorize_url(auth_url: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(auth_url) else {
        return auth_url.to_string();
    };

    let mut query_pairs = parsed
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "originator"
                    | "codex_app_version"
                    | "source_surface_stable_id"
                    | "codex_origin_stable_id"
            )
        })
        .collect::<Vec<_>>();
    let stable_id = official_client_stable_id();
    query_pairs.push(("originator".to_string(), "Codex Desktop".to_string()));
    query_pairs.push(("codex_app_version".to_string(), official_client_version()));
    query_pairs.push(("source_surface_stable_id".to_string(), stable_id.clone()));
    query_pairs.push(("codex_origin_stable_id".to_string(), stable_id));

    {
        let mut query = parsed.query_pairs_mut();
        query.clear();
        for (key, value) in query_pairs {
            query.append_pair(&key, &value);
        }
    }
    parsed.to_string()
}

/// 用户设置优先；留空时使用远端配置缓存，并在无缓存时回退内置默认值。
fn official_client_version() -> String {
    let configured = crate::modules::config::get_user_config().codex_oauth_app_version;
    if let Some(version) =
        crate::modules::remote_config::normalize_codex_oauth_app_version(&configured)
    {
        return version;
    }
    crate::modules::remote_config::cached_codex_oauth_app_version()
}

/// 为两个 stable ID 提供同一个持久化值；它们用于官方登录包装的客户端关联标识。
fn official_client_stable_id() -> String {
    if let Ok(mut cached) = OFFICIAL_CLIENT_STABLE_ID.lock() {
        if let Some(value) = cached.as_ref() {
            return value.clone();
        }

        let path = crate::modules::account::get_data_dir()
            .ok()
            .map(|dir| dir.join(OFFICIAL_CLIENT_IDENTITY_FILE));
        if let Some(path) = path.as_ref() {
            if let Ok(value) = fs::read_to_string(path) {
                let value = value.trim();
                if !value.is_empty() {
                    let value = value.to_string();
                    *cached = Some(value.clone());
                    return value;
                }
            }
        }

        let value = Uuid::new_v4().to_string();
        if let Some(path) = path {
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let _ = fs::write(path, &value);
        }
        *cached = Some(value.clone());
        return value;
    }

    Uuid::new_v4().to_string()
}

lazy_static::lazy_static! {
    static ref OFFICIAL_LOGIN_SESSION: std::sync::Mutex<Option<OfficialLoginSession>> =
        std::sync::Mutex::new(None);
}

/// 启动官方 app-server 的 ChatGPT OAuth 会话，并返回官方生成的 loginId/authUrl。
/// app-server 会继续持有 localhost 回调和 Token 交换，调用方只负责打开 authUrl。
pub fn start_official_login_session(
    codex_home: &Path,
    app_handle: &tauri::AppHandle,
) -> Result<(String, String), String> {
    if let Some(existing) = OFFICIAL_LOGIN_SESSION
        .lock()
        .map_err(|_| "获取官方 OAuth 会话锁失败".to_string())?
        .as_ref()
    {
        return Ok((existing.login_id.clone(), existing.auth_url.clone()));
    }

    let executable = official_app_server_executable()?;
    let mut child = build_app_server_command(&executable, codex_home)
        .spawn()
        .map_err(|error| format!("启动官方 app-server OAuth 会话失败: {}", error))?;
    let stdout = child
        .stdout
        .take()
        .ok_or("无法读取官方 app-server OAuth stdout")?;
    let stderr = child
        .stderr
        .take()
        .ok_or("无法读取官方 app-server OAuth stderr")?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or("无法写入官方 app-server OAuth stdin")?;
    let (sender, receiver) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = sender.send(line);
        }
    });
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            crate::modules::logger::log_warn(&format!(
                "[Codex Official AppServer][oauth stderr] {}",
                line
            ));
        }
    });

    let startup = (|| -> Result<(String, String), String> {
        send_request(
            &mut stdin,
            json!({
                "method": "initialize",
                "id": 1,
                "params": {
                    "clientInfo": {"name": "cockpit-tools", "version": env!("CARGO_PKG_VERSION")},
                    "capabilities": null,
                },
            }),
        )?;
        wait_for_response(&receiver, 1)?;
        send_request(&mut stdin, json!({"method": "initialized", "params": {}}))?;
        send_request(
            &mut stdin,
            json!({
                "method": "account/login/start",
                "id": 2,
                "params": {
                    "type": "chatgpt",
                    "codexStreamlinedLogin": true,
                    "appBrand": "chatgpt",
                    "useHostedLoginSuccessPage": true,
                },
            }),
        )?;
        let response = wait_for_response_value(&receiver, 2)?;
        let result = response
            .get("result")
            .ok_or("官方 app-server OAuth 响应缺少 result")?;
        let login_id = result
            .get("loginId")
            .and_then(JsonValue::as_str)
            .ok_or("官方 app-server OAuth 响应缺少 loginId")?
            .to_string();
        let auth_url = result
            .get("authUrl")
            .and_then(JsonValue::as_str)
            .ok_or("官方 app-server OAuth 响应缺少 authUrl")?
            .to_string();
        Ok((login_id, auth_url))
    })();

    let (login_id, auth_url) = match startup {
        Ok(value) => value,
        Err(error) => {
            finish_child(&mut child);
            return Err(error);
        }
    };
    let auth_url = hosted_auth_url(&auth_url);
    let completion = std::sync::Arc::new((std::sync::Mutex::new(None), std::sync::Condvar::new()));
    let completion_for_reader = completion.clone();
    let app_for_reader = app_handle.clone();
    let login_id_for_reader = login_id.clone();
    std::thread::spawn(move || {
        while let Ok(line) = receiver.recv() {
            let Ok(value) = serde_json::from_str::<JsonValue>(&line) else {
                continue;
            };
            if value.get("method").and_then(JsonValue::as_str) != Some("account/login/completed") {
                continue;
            }
            let params = value.get("params").cloned().unwrap_or_else(|| json!({}));
            if params.get("loginId").and_then(JsonValue::as_str)
                != Some(login_id_for_reader.as_str())
            {
                continue;
            }
            let result = if params.get("success").and_then(JsonValue::as_bool) == Some(true) {
                Ok(())
            } else {
                Err(params
                    .get("error")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("官方 OAuth 登录失败")
                    .to_string())
            };
            if let Ok(mut guard) = completion_for_reader.0.lock() {
                *guard = Some(result.clone());
                completion_for_reader.1.notify_all();
            }
            let _ = tauri::Emitter::emit(
                &app_for_reader,
                "codex-oauth-login-completed",
                serde_json::json!({"loginId": login_id_for_reader}),
            );
            let _ = tauri::Emitter::emit(
                &app_for_reader,
                "ghcp-oauth-login-completed",
                serde_json::json!({"loginId": login_id_for_reader}),
            );
            break;
        }
        if let Ok(mut guard) = completion_for_reader.0.lock() {
            if guard.is_none() {
                *guard = Some(Err("官方 app-server OAuth 会话已断开".to_string()));
                completion_for_reader.1.notify_all();
            }
        }
    });
    crate::modules::logger::log_info(&format!(
        "[Codex Official AppServer] OAuth 会话已启动: login_id={}, pid={}, codex_home={}",
        login_id,
        child.id(),
        codex_home.display()
    ));
    OFFICIAL_LOGIN_SESSION
        .lock()
        .map_err(|_| "获取官方 OAuth 会话锁失败".to_string())?
        .replace(OfficialLoginSession {
            login_id: login_id.clone(),
            auth_url: auth_url.clone(),
            completion,
            stdin,
            child,
        });
    Ok((login_id, auth_url))
}

pub fn official_login_matches(auth_url: &str) -> bool {
    OFFICIAL_LOGIN_SESSION
        .lock()
        .ok()
        .and_then(|guard| guard.as_ref().map(|s| s.auth_url == auth_url))
        .unwrap_or(false)
}

pub fn official_login_matches_login_id(login_id: &str) -> bool {
    OFFICIAL_LOGIN_SESSION
        .lock()
        .ok()
        .and_then(|guard| guard.as_ref().map(|s| s.login_id == login_id))
        .unwrap_or(false)
}

/// 等待官方 app-server 发出的 account/login/completed，并在成功后结束子进程。
pub fn complete_official_login(login_id: &str) -> Result<(), String> {
    let mut session = OFFICIAL_LOGIN_SESSION
        .lock()
        .map_err(|_| "获取官方 OAuth 会话锁失败".to_string())?
        .take()
        .ok_or("官方 OAuth 会话不存在，请重新发起授权")?;
    if session.login_id != login_id {
        finish_child(&mut session.child);
        return Err("官方 OAuth loginId 不匹配".to_string());
    }
    let result = {
        let (lock, condvar) = &*session.completion;
        let guard = lock
            .lock()
            .map_err(|_| "获取官方 OAuth 完成状态锁失败".to_string())?;
        let (guard, _) = condvar
            .wait_timeout_while(guard, OFFICIAL_LOGIN_TIMEOUT, |value| value.is_none())
            .map_err(|_| "等待官方 OAuth 完成状态失败".to_string())?;
        guard
            .clone()
            .unwrap_or_else(|| Err("等待官方 OAuth 登录完成超时".to_string()))
    };
    finish_child(&mut session.child);
    result
}

pub fn cancel_official_login(login_id: Option<&str>) -> Result<bool, String> {
    let mut session = OFFICIAL_LOGIN_SESSION
        .lock()
        .map_err(|_| "获取官方 OAuth 会话锁失败".to_string())?;
    let Some(mut current) = session.take() else {
        return Ok(false);
    };
    if let Some(expected) = login_id {
        if current.login_id != expected {
            *session = Some(current);
            return Err("官方 OAuth loginId 不匹配".to_string());
        }
    }
    finish_child(&mut current.child);
    Ok(true)
}

pub fn rebuild_thread_metadata(codex_home: &Path) -> Result<(), String> {
    let flow_started = Instant::now();
    crate::modules::logger::log_info(&format!(
        "[Codex Official AppServer] rebuild_thread_metadata flow started: codex_home={}",
        codex_home.display()
    ));
    let sanitize_started = Instant::now();
    crate::modules::codex_config_format::sanitize_codex_config_toml_file(
        &codex_home.join("config.toml"),
    )?;
    crate::modules::logger::log_info(&format!(
        "[Codex Official AppServer] sanitize config finished: codex_home={}, elapsed_ms={}, total_ms={}",
        codex_home.display(),
        sanitize_started.elapsed().as_millis(),
        flow_started.elapsed().as_millis()
    ));
    let executable_started = Instant::now();
    let executable = official_app_server_executable()?;
    crate::modules::logger::log_info(&format!(
        "[Codex Official AppServer] executable resolved: executable={}, elapsed_ms={}, total_ms={}",
        executable.display(),
        executable_started.elapsed().as_millis(),
        flow_started.elapsed().as_millis()
    ));
    crate::modules::logger::log_info(&format!(
        "[Codex Official AppServer] starting rebuild_thread_metadata: executable={}, codex_home={}",
        executable.display(),
        codex_home.display()
    ));
    let spawn_started = Instant::now();
    let mut child = build_app_server_command(&executable, codex_home)
        .spawn()
        .map_err(|error| {
            format!(
                "启动官方 Codex app-server 失败 ({} / CODEX_HOME={}): {}",
                executable.display(),
                codex_home.display(),
                error
            )
        })?;
    crate::modules::logger::log_info(&format!(
        "[Codex Official AppServer] child spawned: codex_home={}, pid={:?}, elapsed_ms={}, total_ms={}",
        codex_home.display(),
        child.id(),
        spawn_started.elapsed().as_millis(),
        flow_started.elapsed().as_millis()
    ));

    let stdout = child
        .stdout
        .take()
        .ok_or("无法读取官方 app-server stdout")?;
    let stderr = child
        .stderr
        .take()
        .ok_or("无法读取官方 app-server stderr")?;
    let mut stdin = child.stdin.take().ok_or("无法写入官方 app-server stdin")?;
    let (sender, receiver) = mpsc::channel::<String>();
    let reader = std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            let _ = sender.send(line);
        }
    });
    let stderr_reader = std::thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            crate::modules::logger::log_warn(&format!(
                "[Codex Official AppServer][stderr] {}",
                line
            ));
        }
    });

    let result = (|| {
        let initialize_started = Instant::now();
        send_request(
            &mut stdin,
            json!({
                "method": "initialize",
                "id": 1,
                "params": {
                    "clientInfo": {
                        "name": "cockpit-tools",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "capabilities": null,
                },
            }),
        )?;
        wait_for_response(&receiver, 1)?;
        crate::modules::logger::log_info(&format!(
            "[Codex Official AppServer] initialize finished: codex_home={}, elapsed_ms={}, total_ms={}",
            codex_home.display(),
            initialize_started.elapsed().as_millis(),
            flow_started.elapsed().as_millis()
        ));

        let thread_list_started = Instant::now();
        send_request(
            &mut stdin,
            json!({
                "method": "thread/list",
                "id": 2,
                "params": {
                    "cursor": null,
                    "limit": 1,
                    "sortKey": "updated_at",
                    "sortDirection": "desc",
                    "modelProviders": null,
                    "sourceKinds": [],
                    "archived": false,
                },
            }),
        )?;
        wait_for_response(&receiver, 2)?;
        crate::modules::logger::log_info(&format!(
            "[Codex Official AppServer] thread/list finished: codex_home={}, elapsed_ms={}, total_ms={}",
            codex_home.display(),
            thread_list_started.elapsed().as_millis(),
            flow_started.elapsed().as_millis()
        ));
        Ok::<(), String>(())
    })();

    let finish_started = Instant::now();
    finish_child(&mut child);
    let _ = reader.join();
    let _ = stderr_reader.join();
    crate::modules::logger::log_info(&format!(
        "[Codex Official AppServer] child finished: codex_home={}, elapsed_ms={}, total_ms={}",
        codex_home.display(),
        finish_started.elapsed().as_millis(),
        flow_started.elapsed().as_millis()
    ));
    let result = result.and_then(|()| {
        let normalized_count =
            crate::modules::codex_session_visibility::normalize_official_thread_cwds(codex_home)?;
        if normalized_count > 0 {
            crate::modules::logger::log_info(&format!(
                "[Codex Official AppServer] normalized {} Desktop thread cwd row(s): codex_home={}",
                normalized_count,
                codex_home.display()
            ));
        }
        Ok(())
    });
    if let Err(error) = &result {
        crate::modules::logger::log_warn(&format!(
            "[Codex Official AppServer] rebuild_thread_metadata failed: codex_home={}, elapsed_ms={}, error={}",
            codex_home.display(),
            flow_started.elapsed().as_millis(),
            error
        ));
    } else {
        crate::modules::logger::log_info(&format!(
            "[Codex Official AppServer] rebuild_thread_metadata completed: codex_home={}, elapsed_ms={}",
            codex_home.display(),
            flow_started.elapsed().as_millis()
        ));
    }
    result
}

pub(crate) fn official_app_server_executable() -> Result<PathBuf, String> {
    let mut candidates = Vec::new();
    if let Some(executable) = std::env::var_os(CODEX_APP_SERVER_EXECUTABLE_ENV) {
        if !executable.as_os_str().is_empty() {
            push_candidate(&mut candidates, PathBuf::from(executable));
        }
    }
    add_codex_app_server_candidates(&mut candidates);

    for executable in &candidates {
        if executable.exists() {
            return Ok(executable.clone());
        }
    }

    let searched_paths = candidates
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!(
        "未找到官方 Codex app-server 可执行文件: {}",
        searched_paths
    ))
}

fn add_codex_app_server_candidates(candidates: &mut Vec<PathBuf>) {
    let configured_path = crate::modules::config::get_user_config().codex_app_path;
    if !configured_path.trim().is_empty() {
        push_candidate_from_codex_launch_path(candidates, Path::new(configured_path.trim()));
    }

    if let Some(detected_path) = crate::modules::process::detect_codex_exec_path() {
        push_candidate_from_codex_launch_path(candidates, &detected_path);
    }

    #[cfg(target_os = "macos")]
    for executable in CODEX_APP_SERVER_MACOS_EXECUTABLES {
        push_candidate(candidates, PathBuf::from(executable));
    }
}

fn push_candidate_from_codex_launch_path(candidates: &mut Vec<PathBuf>, launch_path: &Path) {
    if let Some(app_server_path) = app_server_executable_from_codex_launch_path(launch_path) {
        push_candidate(candidates, app_server_path);
    }
}

fn push_candidate(candidates: &mut Vec<PathBuf>, path: PathBuf) {
    if path.as_os_str().is_empty() || candidates.iter().any(|candidate| candidate == &path) {
        return;
    }
    candidates.push(path);
}

fn app_server_executable_from_codex_launch_path(path: &Path) -> Option<PathBuf> {
    if path.as_os_str().is_empty() {
        return None;
    }

    if is_existing_app_server_path_shape(path) {
        return Some(path.to_path_buf());
    }

    if path_file_name_eq(path, "codex.app") {
        return Some(path.join("Contents").join("Resources").join("codex"));
    }

    if path_file_name_eq(path, "chatgpt.app") {
        return Some(path.join("Contents").join("Resources").join("codex"));
    }

    if path_file_name_eq(path, "codex") && parent_file_name_eq(path, "macos") {
        let contents_dir = path.parent()?.parent()?;
        return Some(contents_dir.join("Resources").join("codex"));
    }

    if path_file_name_eq(path, "chatgpt") && parent_file_name_eq(path, "macos") {
        let contents_dir = path.parent()?.parent()?;
        return Some(contents_dir.join("Resources").join("codex"));
    }

    let resolved = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    if path_file_name_eq(&resolved, "chatgpt") && parent_file_name_eq(&resolved, "chatgpt") {
        return Some(resolved.parent()?.join("resources").join("codex"));
    }

    if path_file_name_eq(path, "codex.exe") {
        return Some(path.parent()?.join("resources").join("codex.exe"));
    }

    if path_file_name_eq(path, "chatgpt.exe") {
        return Some(path.parent()?.join("resources").join("codex.exe"));
    }

    None
}

fn is_existing_app_server_path_shape(path: &Path) -> bool {
    if path_file_name_eq(path, "codex") && parent_file_name_eq(path, "resources") {
        return true;
    }
    path_file_name_eq(path, "codex.exe") && parent_file_name_eq(path, "resources")
}

fn path_file_name_eq(path: &Path, expected: &str) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(|value| value.eq_ignore_ascii_case(expected))
        .unwrap_or(false)
}

fn parent_file_name_eq(path: &Path, expected: &str) -> bool {
    path.parent()
        .and_then(Path::file_name)
        .and_then(|value| value.to_str())
        .map(|value| value.eq_ignore_ascii_case(expected))
        .unwrap_or(false)
}

fn build_app_server_command(executable: &Path, codex_home: &Path) -> Command {
    let mut command = Command::new(executable);
    crate::modules::process::apply_managed_proxy_env_to_command(&mut command);
    command
        .args(["app-server", "--listen", "stdio://"])
        .env("CODEX_HOME", codex_home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    #[cfg(windows)]
    {
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        command.creation_flags(CREATE_NO_WINDOW);
    }

    command
}

fn send_request(stdin: &mut impl Write, request: JsonValue) -> Result<(), String> {
    let line = serde_json::to_string(&request)
        .map_err(|error| format!("序列化官方 app-server 请求失败: {}", error))?;
    stdin
        .write_all(line.as_bytes())
        .and_then(|_| stdin.write_all(b"\n"))
        .and_then(|_| stdin.flush())
        .map_err(|error| format!("写入官方 app-server 请求失败: {}", error))
}

fn wait_for_response(receiver: &mpsc::Receiver<String>, request_id: i64) -> Result<(), String> {
    wait_for_response_value(receiver, request_id).map(|_| ())
}

fn wait_for_response_value(
    receiver: &mpsc::Receiver<String>,
    request_id: i64,
) -> Result<JsonValue, String> {
    loop {
        let line = receiver
            .recv_timeout(APP_SERVER_RESPONSE_TIMEOUT)
            .map_err(|_| format!("等待官方 app-server 响应超时 (id={})", request_id))?;
        let Ok(value) = serde_json::from_str::<JsonValue>(&line) else {
            continue;
        };
        if value.get("id").and_then(JsonValue::as_i64) != Some(request_id) {
            continue;
        }
        if let Some(error) = value.get("error") {
            crate::modules::logger::log_warn(&format!(
                "[Codex Official AppServer] response error: id={}, error={}",
                request_id, error
            ));
            return Err(format!(
                "官方 app-server 返回错误 (id={}): {}",
                request_id, error
            ));
        }
        if value.get("result").is_some() {
            return Ok(value);
        }
        return Err(format!(
            "官方 app-server 响应缺少 result (id={}): {}",
            request_id, value
        ));
    }
}

fn finish_child(child: &mut Child) {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_app_server_authorize_url_like_desktop_client() {
        let authorize_url = "https://auth.openai.com/oauth/authorize?response_type=code&state=a+b";
        let wrapped = hosted_auth_url(authorize_url);
        let parsed = url::Url::parse(&wrapped).expect("parse hosted auth url");

        assert_eq!(parsed.host_str(), Some("chatgpt.com"));
        assert_eq!(parsed.path(), "/codex/desktop-auth");
        let params = parsed
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        let inner = params.get("authorize_url").expect("wrapped authorize_url");
        let inner = url::Url::parse(inner).expect("parse inner authorize url");
        let inner_params = inner
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            inner_params.get("response_type").map(|v| v.as_ref()),
            Some("code")
        );
        assert_eq!(inner_params.get("state").map(|v| v.as_ref()), Some("a b"));
        assert_eq!(
            params.get("codex_streamlined_login").map(|v| v.as_ref()),
            Some("true")
        );
        assert_eq!(
            params.get("no_universal_links").map(|v| v.as_ref()),
            Some("1")
        );

        assert_eq!(
            inner_params.get("originator").map(|v| v.as_ref()),
            Some("Codex Desktop")
        );
        assert!(inner_params.contains_key("codex_app_version"));
        let source_id = inner_params
            .get("source_surface_stable_id")
            .expect("source stable id");
        assert_eq!(inner_params.get("codex_origin_stable_id"), Some(source_id));
    }

    #[test]
    fn enriches_authorize_url_without_replacing_dynamic_parameters() {
        let original = "https://auth.openai.com/oauth/authorize?response_type=code&state=a%2Bb&code_challenge=x&originator=cockpit-tools";
        let enriched = enrich_official_authorize_url(original);
        let parsed = url::Url::parse(&enriched).expect("parse enriched authorize url");
        let params = parsed
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            params.get("response_type").map(|v| v.as_ref()),
            Some("code")
        );
        assert_eq!(params.get("state").map(|v| v.as_ref()), Some("a+b"));
        assert_eq!(params.get("code_challenge").map(|v| v.as_ref()), Some("x"));
        assert_eq!(
            params.get("originator").map(|v| v.as_ref()),
            Some("Codex Desktop")
        );
    }

    #[test]
    fn maps_macos_launch_binary_to_resources_app_server() {
        let launch_path = PathBuf::from("/Applications/Codex.app/Contents/MacOS/Codex");
        let app_server_path = app_server_executable_from_codex_launch_path(&launch_path)
            .expect("resolve app-server path");

        assert_eq!(
            app_server_path,
            PathBuf::from("/Applications/Codex.app/Contents/Resources/codex")
        );
    }

    #[test]
    fn maps_chatgpt_macos_launch_binary_to_resources_app_server() {
        let launch_path = PathBuf::from("/Applications/ChatGPT.app/Contents/MacOS/ChatGPT");
        let app_server_path = app_server_executable_from_codex_launch_path(&launch_path)
            .expect("resolve app-server path");

        assert_eq!(
            app_server_path,
            PathBuf::from("/Applications/ChatGPT.app/Contents/Resources/codex")
        );
    }

    #[test]
    fn maps_macos_app_root_to_resources_app_server() {
        let launch_path = PathBuf::from("/Applications/Codex.app");
        let app_server_path = app_server_executable_from_codex_launch_path(&launch_path)
            .expect("resolve app-server path");

        assert_eq!(
            app_server_path,
            PathBuf::from("/Applications/Codex.app/Contents/Resources/codex")
        );
    }

    #[test]
    fn maps_chatgpt_macos_app_root_to_resources_app_server() {
        let launch_path = PathBuf::from("/Applications/ChatGPT.app");
        let app_server_path = app_server_executable_from_codex_launch_path(&launch_path)
            .expect("resolve app-server path");

        assert_eq!(
            app_server_path,
            PathBuf::from("/Applications/ChatGPT.app/Contents/Resources/codex")
        );
    }

    #[test]
    fn maps_windows_launch_binary_to_resources_app_server() {
        let launch_path =
            PathBuf::from("C:/Program Files/WindowsApps/OpenAI.Codex_1.2.3/app/Codex.exe");
        let app_server_path = app_server_executable_from_codex_launch_path(&launch_path)
            .expect("resolve app-server path");

        assert_eq!(
            app_server_path,
            PathBuf::from(
                "C:/Program Files/WindowsApps/OpenAI.Codex_1.2.3/app/resources/codex.exe"
            )
        );
    }

    #[test]
    fn maps_chatgpt_windows_launch_binary_to_resources_app_server() {
        let launch_path =
            PathBuf::from("C:/Program Files/WindowsApps/OpenAI.ChatGPT_1.2.3/app/ChatGPT.exe");
        let app_server_path = app_server_executable_from_codex_launch_path(&launch_path)
            .expect("resolve app-server path");

        assert_eq!(
            app_server_path,
            PathBuf::from(
                "C:/Program Files/WindowsApps/OpenAI.ChatGPT_1.2.3/app/resources/codex.exe"
            )
        );
    }

    #[test]
    fn keeps_existing_resources_app_server_path() {
        let app_server_path = PathBuf::from(
            "C:/Program Files/WindowsApps/OpenAI.Codex_1.2.3/app/resources/codex.exe",
        );

        assert_eq!(
            app_server_executable_from_codex_launch_path(&app_server_path),
            Some(app_server_path)
        );
    }

    #[test]
    fn maps_linux_chatgpt_binary_to_resources_app_server() {
        let launch_path = PathBuf::from("/usr/lib/chatgpt/ChatGPT");
        assert_eq!(
            app_server_executable_from_codex_launch_path(&launch_path),
            Some(PathBuf::from("/usr/lib/chatgpt/resources/codex"))
        );
    }
}
