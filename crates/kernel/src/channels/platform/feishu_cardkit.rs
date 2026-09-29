//! 飞书 `CardKit` 请求构造（chat-flow 增量 8，P5 通用内核的纯构造
//! 部分）。设计依据 docs/design/chat-flow-technical-design.md §8：
//! `CardKit` 实体自创建起 14 天有效，是 L1 换代「最早适用更新期
//! 限」的判据之一（`card_entity_created_at` 预留列的数据源）。
//!
//! **API 存在性有官方文档，行为未实测（P0 门槛）**——本模块只
//! 为 `FeishuAdapter` 加三个方法：构造请求并经既有 request 助手
//! （`api_post`/`api_json`）发送；逐方法标注「未实测」。本增量
//! 不接线任何调用方（渲染 v2/局部更新属平台行为主导，真卡凭据
//! 到位后再接）。
//!
//! 请求形态依据 2026-09-30 复核的官方文档（open.feishu.cn
//! `document_portal` API schema）：
//! - 创建卡片实体：`POST /open-apis/cardkit/v1/cards`，body
//!   `{type: "card_json", data: "<转义为字符串的卡片 JSON>"}`，
//!   响应 `data.card_id`；
//! - 局部更新卡片实体：`POST /open-apis/cardkit/v1/cards/{card_id}
//!   /batch_update`（官方方法是 POST，规格草案写 PUT——以官方
//!   为准），body `{uuid, sequence, actions: "<转义为字符串的操
//!   作数组>"}`；
//! - 流式更新文本（全量更新组件文本内容）：`PUT /open-apis/
//!   cardkit/v1/cards/{card_id}/elements/{element_id}/content`
//!   （官方方法是 PUT；PATCH 作用于 `/elements/{element_id}` 的
//!   `partial_element`——规格草案的 PATCH+`/content` 组合不存
//!   在，以官方为准），body `{content, sequence}`。

use serde_json::json;

use crate::channels::ChannelError;

use super::{resp_data_str, FeishuAdapter};

// 本增量刻意不接线任何调用方（渲染 v2 凭据到位后再接，§7.2 P0
// 门槛）——方法仅供请求构造与单测，非 test 构建下无调用方。
#[allow(dead_code)]
impl FeishuAdapter {
    /// 创建卡片实体（CardKit）。**未实测（P0 门槛）**：API 存在
    /// 性有官方文档（cardkit-v1/card/create），真实行为待真卡凭
    /// 据验证。`card_json` 是卡片 JSON 2.0 原文（官方要求转义为
    /// 字符串传入 `data`）。返回实体 `card_id`——14 天期限从创
    /// 建起计（L1 换代 `card_entity_created_at` 的数据源）。
    pub(crate) async fn cardkit_create(
        &self,
        card_json: &str,
    ) -> Result<Option<String>, ChannelError> {
        let token = self.get_token().await?;
        let resp = self
            .api_post(
                &token,
                &format!("{}/open-apis/cardkit/v1/cards", self.base_url),
                json!({ "type": "card_json", "data": card_json }),
            )
            .await?;
        Ok(resp_data_str(&resp, "card_id"))
    }

    /// 局部更新卡片实体（`batch_update`）。**未实测（P0 门槛）**：
    /// API 存在性有官方文档（cardkit-v1/card/batch_update），真
    /// 实行为待真卡凭据验证。`updates` 是操作列表（官方
    /// `actions` 字段的操作数组——`partial_update_setting`/
    /// `add_elements`/`delete_elements`/`partial_update_element`/
    /// `update_element`），按官方要求转义为字符串入 body；
    /// `sequence` 递增（同一实体版本只向前）；`uuid` 幂等（相同
    /// 批次操作只执行一次）。
    pub(crate) async fn cardkit_batch_update(
        &self,
        card_id: &str,
        updates: &serde_json::Value,
        sequence: i64,
        uuid: &str,
    ) -> Result<(), ChannelError> {
        let token = self.get_token().await?;
        self.api_post(
            &token,
            &format!(
                "{}/open-apis/cardkit/v1/cards/{card_id}/batch_update",
                self.base_url
            ),
            json!({
                "uuid": uuid,
                "sequence": sequence,
                "actions": updates.to_string(),
            }),
        )
        .await?;
        Ok(())
    }

    /// 流式更新文本（全量更新文本元素/富文本组件的内容）。
    /// **未实测（P0 门槛）**：API 存在性有官方文档
    /// （cardkit-v1/card-element/content），真实行为待真卡凭据
    /// 验证。`content` 是全量文本（官方语义是「打字机」式输
    /// 出）；`sequence` 递增（同一实体版本只向前）。
    pub(crate) async fn cardkit_element_content_update(
        &self,
        card_id: &str,
        element_id: &str,
        content: &str,
        sequence: i64,
    ) -> Result<(), ChannelError> {
        let token = self.get_token().await?;
        self.api_json(
            self.client.put(format!(
                "{}/open-apis/cardkit/v1/cards/{card_id}/elements/{element_id}/content",
                self.base_url
            )),
            &token,
            json!({ "content": content, "sequence": sequence }),
        )
        .await?;
        Ok(())
    }
}
