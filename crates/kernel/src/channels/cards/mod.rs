//! 交互面板卡：settings（chat 配置）、cron_card（定时任务）、
//! mailbox（待处理消息）、approval（权限审批）、ask（决策卡）、
//! welcome（入群欢迎）、taskcard（执行任务占位主卡）。

pub(crate) mod approval;
pub(crate) mod ask;
pub(crate) mod cron_card;
pub(crate) mod mailbox;
pub(crate) mod settings;
pub(crate) mod taskcard;
pub(crate) mod welcome;
