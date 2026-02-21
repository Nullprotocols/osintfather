import os
import asyncpg
import re
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Any, Union

DATABASE_URL = os.getenv("DATABASE_URL")   # Render automatically provides this

_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    """Return connection pool (singleton)"""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    return _pool

# -------------------------------------------------------------------
# Helper function (unchanged)
# -------------------------------------------------------------------
def parse_time_string(time_str: Union[str, None]) -> Union[int, None]:
    """
    Parse time string like: 
    "30m" = 30 minutes
    "2h" = 2 hours (120 minutes)
    "1h30m" = 90 minutes
    "24h" = 1440 minutes
    """
    if not time_str or str(time_str).lower() == 'none':
        return None
    
    time_str = str(time_str).lower()
    total_minutes = 0
    
    # Extract hours
    hour_match = re.search(r'(\d+)h', time_str)
    if hour_match:
        total_minutes += int(hour_match.group(1)) * 60
    
    # Extract minutes
    minute_match = re.search(r'(\d+)m', time_str)
    if minute_match:
        total_minutes += int(minute_match.group(1))
    
    # If no h/m specified, assume minutes if it's a number
    if not hour_match and not minute_match and time_str.isdigit():
        total_minutes = int(time_str)
    
    return total_minutes if total_minutes > 0 else None

# -------------------------------------------------------------------
# Database initialization
# -------------------------------------------------------------------
async def init_db():
    """Create tables if not exist"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Users Table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                credits INTEGER DEFAULT 5,
                joined_date TEXT,
                referrer_id BIGINT,
                is_banned INTEGER DEFAULT 0,
                total_earned INTEGER DEFAULT 0,
                last_active TEXT
            )
        """)
        
        # Admins Table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                level TEXT DEFAULT 'admin',
                added_by BIGINT,
                added_date TEXT
            )
        """)
        
        # Redeem Codes Table with expiry in MINUTES
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS redeem_codes (
                code TEXT PRIMARY KEY,
                amount INTEGER,
                max_uses INTEGER,
                current_uses INTEGER DEFAULT 0,
                expiry_minutes INTEGER,
                created_date TEXT,
                is_active INTEGER DEFAULT 1
            )
        """)
        
        # Redeem logs to track who used which code
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS redeem_logs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                code TEXT,
                claimed_date TEXT,
                UNIQUE(user_id, code)
            )
        """)
        
        # Statistics Table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                date TEXT PRIMARY KEY,
                total_users INTEGER DEFAULT 0,
                active_users INTEGER DEFAULT 0,
                total_lookups INTEGER DEFAULT 0,
                credits_used INTEGER DEFAULT 0
            )
        """)
        
        # Lookup Logs Table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lookup_logs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                api_type TEXT,
                input_data TEXT,
                result TEXT,
                lookup_date TEXT
            )
        """)

# -------------------------------------------------------------------
# User management
# -------------------------------------------------------------------
async def get_user(user_id: int) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

async def add_user(user_id: int, username: str, referrer_id: Optional[int] = None):
    pool = await get_pool()
    # Check if user exists
    exists = await pool.fetchval("SELECT 1 FROM users WHERE user_id = $1", user_id)
    if exists:
        return
    
    current_time = str(datetime.now().timestamp())
    await pool.execute("""
        INSERT INTO users (user_id, username, credits, joined_date, referrer_id, is_banned, total_earned, last_active) 
        VALUES ($1, $2, 5, $3, $4, 0, 0, $3)
    """, user_id, username, current_time, referrer_id)

async def update_credits(user_id: int, amount: int):
    pool = await get_pool()
    if amount > 0:
        await pool.execute("""
            UPDATE users SET credits = credits + $1, total_earned = total_earned + $1 
            WHERE user_id = $2
        """, amount, user_id)
    else:
        await pool.execute("UPDATE users SET credits = credits + $1 WHERE user_id = $2", amount, user_id)

async def set_ban_status(user_id: int, status: int):
    pool = await get_pool()
    await pool.execute("UPDATE users SET is_banned = $1 WHERE user_id = $2", status, user_id)

async def get_all_users() -> List[int]:
    pool = await get_pool()
    rows = await pool.fetch("SELECT user_id FROM users")
    return [row['user_id'] for row in rows]

async def get_user_by_username(username: str) -> Optional[int]:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT user_id FROM users WHERE username = $1", username)
    return row['user_id'] if row else None

async def update_username(user_id: int, username: str):
    pool = await get_pool()
    await pool.execute("UPDATE users SET username = $1 WHERE user_id = $2", username, user_id)

async def get_user_stats(user_id: int) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetchrow("""
        SELECT 
            (SELECT COUNT(*) FROM users WHERE referrer_id = $1) as referrals,
            (SELECT COUNT(*) FROM redeem_logs WHERE user_id = $1) as codes_claimed,
            (SELECT COALESCE(SUM(rc.amount), 0) FROM redeem_logs rl 
             JOIN redeem_codes rc ON rl.code = rc.code 
             WHERE rl.user_id = $1) as total_from_codes
    """, user_id)

async def get_user_by_id(user_id: int) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

async def search_users(query: str) -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT user_id, username, credits 
        FROM users 
        WHERE username ILIKE $1 OR user_id::text = $2
        LIMIT 20
    """, f"%{query}%", query if query.isdigit() else "0")

async def delete_user(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM users WHERE user_id = $1", user_id)
        await conn.execute("DELETE FROM redeem_logs WHERE user_id = $1", user_id)
        await conn.execute("UPDATE users SET referrer_id = NULL WHERE referrer_id = $1", user_id)

async def reset_user_credits(user_id: int):
    pool = await get_pool()
    await pool.execute("UPDATE users SET credits = 0 WHERE user_id = $1", user_id)

async def get_recent_users(limit: int = 20) -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT user_id, username, joined_date 
        FROM users 
        ORDER BY joined_date DESC 
        LIMIT $1
    """, limit)

async def get_premium_users() -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT user_id, username, credits 
        FROM users 
        WHERE credits >= 100
        ORDER BY credits DESC
    """)

async def get_low_credit_users() -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT user_id, username, credits 
        FROM users 
        WHERE credits <= 5
        ORDER BY credits ASC
    """)

async def get_inactive_users(days: int = 30) -> List[asyncpg.Record]:
    pool = await get_pool()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    return await pool.fetch("""
        SELECT user_id, username, last_active 
        FROM users 
        WHERE last_active < $1 
        AND is_banned = 0
        ORDER BY last_active ASC
    """, cutoff)

async def update_last_active(user_id: int):
    pool = await get_pool()
    await pool.execute("UPDATE users SET last_active = $1 WHERE user_id = $2", 
                       datetime.now().isoformat(), user_id)

async def get_user_activity(user_id: int, days: int = 7) -> int:
    pool = await get_pool()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    return await pool.fetchval("""
        SELECT COUNT(*) 
        FROM lookup_logs 
        WHERE user_id = $1 AND lookup_date > $2
    """, user_id, cutoff)

async def get_leaderboard(limit: int = 10) -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT user_id, username, credits 
        FROM users 
        WHERE is_banned = 0
        ORDER BY credits DESC 
        LIMIT $1
    """, limit)

async def bulk_update_credits(user_ids: List[int], amount: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for uid in user_ids:
                if amount > 0:
                    await conn.execute("""
                        UPDATE users SET credits = credits + $1, total_earned = total_earned + $1 
                        WHERE user_id = $2
                    """, amount, uid)
                else:
                    await conn.execute("UPDATE users SET credits = credits + $1 WHERE user_id = $2", amount, uid)

# -------------------------------------------------------------------
# Referral system
# -------------------------------------------------------------------
async def get_top_referrers(limit: int = 10) -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT referrer_id, COUNT(*) as referrals 
        FROM users 
        WHERE referrer_id IS NOT NULL 
        GROUP BY referrer_id 
        ORDER BY referrals DESC 
        LIMIT $1
    """, limit)

# -------------------------------------------------------------------
# Bot statistics
# -------------------------------------------------------------------
async def get_bot_stats() -> dict:
    pool = await get_pool()
    total_users = await pool.fetchval("SELECT COUNT(*) FROM users")
    active_users = await pool.fetchval("SELECT COUNT(*) FROM users WHERE credits > 0")
    total_credits = await pool.fetchval("SELECT COALESCE(SUM(credits), 0) FROM users")
    credits_distributed = await pool.fetchval("SELECT COALESCE(SUM(total_earned), 0) FROM users")
    return {
        'total_users': total_users,
        'active_users': active_users,
        'total_credits': total_credits,
        'credits_distributed': credits_distributed
    }

async def get_users_in_range(start_date: float, end_date: float) -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT user_id, username, credits, joined_date 
        FROM users 
        WHERE joined_date::float BETWEEN $1 AND $2
    """, start_date, end_date)

async def get_daily_stats(days: int = 7) -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT 
            date(joined_date, 'unixepoch') as join_date,
            COUNT(*) as new_users,
            (SELECT COUNT(*) FROM redeem_logs 
             WHERE date(claimed_date) = date(joined_date, 'unixepoch')) as claims
        FROM users 
        WHERE date(joined_date, 'unixepoch') >= date('now', $1 || ' days')
        GROUP BY join_date
        ORDER BY join_date DESC
    """, f"-{days}")

# -------------------------------------------------------------------
# Admin management
# -------------------------------------------------------------------
async def add_admin(user_id: int, level: str = 'admin'):
    pool = await get_pool()
    await pool.execute("""
        INSERT INTO admins (user_id, level) VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
    """, user_id, level)

async def remove_admin(user_id: int):
    pool = await get_pool()
    await pool.execute("DELETE FROM admins WHERE user_id = $1", user_id)

async def get_all_admins() -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("SELECT user_id, level FROM admins")

async def is_admin(user_id: int) -> Optional[str]:
    pool = await get_pool()
    level = await pool.fetchval("SELECT level FROM admins WHERE user_id = $1", user_id)
    return level

# -------------------------------------------------------------------
# Redeem codes
# -------------------------------------------------------------------
async def create_redeem_code(code: str, amount: int, max_uses: int, expiry_minutes: Optional[int] = None):
    pool = await get_pool()
    await pool.execute("""
        INSERT INTO redeem_codes (code, amount, max_uses, expiry_minutes, created_date, is_active)
        VALUES ($1, $2, $3, $4, $5, 1)
        ON CONFLICT (code) DO UPDATE SET
            amount = EXCLUDED.amount,
            max_uses = EXCLUDED.max_uses,
            expiry_minutes = EXCLUDED.expiry_minutes,
            created_date = EXCLUDED.created_date,
            is_active = 1
    """, code, amount, max_uses, expiry_minutes, datetime.now().isoformat())

async def redeem_code_db(user_id: int, code: str) -> Union[int, str]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Check if user already claimed this code
        claimed = await conn.fetchval("SELECT 1 FROM redeem_logs WHERE user_id = $1 AND code = $2", user_id, code)
        if claimed:
            return "already_claimed"
        
        # Get code details
        row = await conn.fetchrow("""
            SELECT amount, max_uses, current_uses, expiry_minutes, created_date, is_active
            FROM redeem_codes WHERE code = $1
        """, code)
        if not row:
            return "invalid"
        
        amount, max_uses, current_uses, expiry_minutes, created_date, is_active = row
        if not is_active:
            return "inactive"
        if current_uses >= max_uses:
            return "limit_reached"
        if expiry_minutes is not None and expiry_minutes > 0:
            created_dt = datetime.fromisoformat(created_date)
            if datetime.now() > created_dt + timedelta(minutes=expiry_minutes):
                return "expired"
        
        # All checks passed, process redeem in transaction
        async with conn.transaction():
            await conn.execute("UPDATE redeem_codes SET current_uses = current_uses + 1 WHERE code = $1", code)
            await conn.execute("UPDATE users SET credits = credits + $1, total_earned = total_earned + $1 WHERE user_id = $2", amount, user_id)
            await conn.execute("INSERT INTO redeem_logs (user_id, code, claimed_date) VALUES ($1, $2, $3)", user_id, code, datetime.now().isoformat())
        return amount

async def get_all_codes() -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT code, amount, max_uses, current_uses, expiry_minutes, created_date, is_active
        FROM redeem_codes
        ORDER BY created_date DESC
    """)

async def get_active_codes() -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT code, amount, max_uses, current_uses
        FROM redeem_codes
        WHERE is_active = 1
        ORDER BY created_date DESC
    """)

async def get_inactive_codes() -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT code, amount, max_uses, current_uses
        FROM redeem_codes
        WHERE is_active = 0
        ORDER BY created_date DESC
    """)

async def deactivate_code(code: str):
    pool = await get_pool()
    await pool.execute("UPDATE redeem_codes SET is_active = 0 WHERE code = $1", code)

async def delete_redeem_code(code: str):
    pool = await get_pool()
    await pool.execute("DELETE FROM redeem_codes WHERE code = $1", code)

async def get_expired_codes() -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT code, amount, current_uses, max_uses, expiry_minutes, created_date 
        FROM redeem_codes 
        WHERE is_active = 1 
        AND expiry_minutes IS NOT NULL 
        AND expiry_minutes > 0
        AND (created_date::timestamp + (expiry_minutes * interval '1 minute')) < now()
    """)

async def get_user_redeem_history(user_id: int) -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("SELECT code, claimed_date FROM redeem_logs WHERE user_id = $1", user_id)

async def get_code_usage_stats(code: str) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetchrow("""
        SELECT 
            rc.amount, rc.max_uses, rc.current_uses,
            COUNT(DISTINCT rl.user_id) as unique_users,
            array_agg(DISTINCT rl.user_id) as user_ids
        FROM redeem_codes rc
        LEFT JOIN redeem_logs rl ON rc.code = rl.code
        WHERE rc.code = $1
        GROUP BY rc.code, rc.amount, rc.max_uses, rc.current_uses
    """, code)

# -------------------------------------------------------------------
# Lookup logs
# -------------------------------------------------------------------
async def log_lookup(user_id: int, api_type: str, input_data: str, result: str):
    pool = await get_pool()
    await pool.execute("""
        INSERT INTO lookup_logs (user_id, api_type, input_data, result, lookup_date)
        VALUES ($1, $2, $3, $4, $5)
    """, user_id, api_type, input_data[:500], str(result)[:1000], datetime.now().isoformat())

async def get_lookup_stats(user_id: Optional[int] = None) -> List[asyncpg.Record]:
    pool = await get_pool()
    if user_id:
        return await pool.fetch("""
            SELECT api_type, COUNT(*) as count 
            FROM lookup_logs 
            WHERE user_id = $1
            GROUP BY api_type
        """, user_id)
    else:
        return await pool.fetch("""
            SELECT api_type, COUNT(*) as count 
            FROM lookup_logs 
            GROUP BY api_type
        """)

async def get_total_lookups() -> int:
    pool = await get_pool()
    return await pool.fetchval("SELECT COUNT(*) FROM lookup_logs")

async def get_user_lookups(user_id: int, limit: int = 50) -> List[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch("""
        SELECT api_type, input_data, lookup_date 
        FROM lookup_logs 
        WHERE user_id = $1
        ORDER BY lookup_date DESC
        LIMIT $2
    """, user_id, limit)
