-- 0003_deadlines.sql
-- 可选执行截止时刻（deadline）与到期结算终态 'expired'。
--
-- 设计要点：
--   * commands.deadline_at 为可选 UTC 执行截止时刻；NULL 表示未填写，
--     旧指令无需回填，创建/领取/回执行为与过去完全一致；
--   * status 增加到期终态 'expired'：仅当数据库时钟已到截止点（deadline_at <= now()）
--     且指令尚未送达（仍为 pending）时，由针对指定指令的到期结算在行锁内原子转入。
--     expired 是正式结算（settlements 有一行），但不伪造任何租约回执：
--     generation/lease_token 必须为 NULL，result='expired'；
--   * expired 与 failed 一样按现有前驱闭包规则把尚未送达的后继链原子转为 blocked，
--     blocked_by 记录到期根编号（到期根因）；
--   * 领取资格、回执有效性、到期判定共用同一数据库时钟（now()）与同一行锁裁决：
--     截止前的成功送达不可被追改，截止后旧租约即使尚未达到自身租期也不能送达。

ALTER TABLE commands
    ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ NULL;

-- 重建状态契约为五态（0002 已摘除 0001 的内联约束名，此处替换 0002 的具名约束）。
-- 幂等：先 DROP IF EXISTS，再在不存在时创建。
ALTER TABLE commands DROP CONSTRAINT IF EXISTS commands_status_chk;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'commands_status_chk') THEN
        ALTER TABLE commands
            ADD CONSTRAINT commands_status_chk
            CHECK (status IN ('pending', 'delivered', 'failed', 'blocked', 'expired'));
    END IF;
END$$;

-- settlements.result 增加 'expired'：0001 的内联约束自动名为 settlements_result_check。
ALTER TABLE settlements DROP CONSTRAINT IF EXISTS settlements_result_check;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settlements_result_chk') THEN
        ALTER TABLE settlements
            ADD CONSTRAINT settlements_result_chk
            CHECK (result IN ('delivered', 'failed', 'expired'));
    END IF;
END$$;

-- 到期结算不来自任何租约令牌：generation/lease_token 对 expired 行必须为 NULL；
-- delivered/failed 仍必须记录签发代次与令牌。两方向同生同灭。
-- 旧行的两列为 NOT NULL，先解除再以约束统一契约（重复迁移幂等）。
ALTER TABLE settlements ALTER COLUMN generation DROP NOT NULL;
ALTER TABLE settlements ALTER COLUMN lease_token DROP NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settlements_expired_null_chk') THEN
        ALTER TABLE settlements
            ADD CONSTRAINT settlements_expired_null_chk
            CHECK ((result = 'expired') = (generation IS NULL AND lease_token IS NULL));
    END IF;
END$$;
