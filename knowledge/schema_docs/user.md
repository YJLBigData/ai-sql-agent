# dim_user

- 表名: `dim_user`
- 粒度: 一行一位用户
- 主键: `user_id`
- 常用字段:
  - `user_id`: 用户唯一标识
  - `register_date`: 注册日期
  - `region_name` / `province_name` / `city_name`: 用户地域
  - `source_channel`: 用户来源渠道
  - `member_level`: 会员等级
  - `is_member`: 是否会员
- 典型用途:
  - 看新增用户、会员结构、用户地域分布
  - 和 `fct_order_main.user_id` 关联做用户成交分析

