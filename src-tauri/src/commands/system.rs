// System commands 统一入口。
// 按配置模型、备份/WebDAV、网络/通用设置和应用命令职责拆分，
// 通过 include! 保持 Tauri command 注册路径与调用行为不变。
include!("system_config_types.rs");
include!("system_backup_webdav.rs");
include!("system_network_general.rs");
include!("system_app_commands.rs");

#[cfg(test)]
mod tests {
    include!("system_tests.rs");
}
