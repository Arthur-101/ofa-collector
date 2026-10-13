import db_supabase

def inspect():
    c = db_supabase.get_supabase()
    res = c.table('signals').select('*').gte('fired_at', '2026-10-13T03:30:00').lte('fired_at', '2026-10-13T08:00:00').limit(5).execute()
    sigs = res.data or []
    for s in sigs:
        sig_id = s['id']
        fired_at = s['fired_at']
        strike = s['strike']
        opt = s['option_type']
        exp = s['expiry']
        bias = s['bias']
        print(f"=== Signal {sig_id}: {fired_at} | {strike} {opt} ({exp}) | Bias: {bias} ===")
        print(f"  Old Stored: Entry={s.get('entry_premium')} Exit={s.get('exit_premium')} Reason={s.get('exit_reason')} PnL={s.get('net_pnl_pct')}%")
        
        ticks = c.table('options_chain').select('timestamp,last_price,volume').eq('expiry', exp).eq('strike', strike).eq('option_type', opt).gt('timestamp', fired_at).order('timestamp', desc=False).limit(6).execute().data
        print("  Raw Ticks in options_chain (strictly after fired_at):")
        for t in ticks:
            print(f"    {t['timestamp']} -> LTP: {t['last_price']} | Vol: {t['volume']}")

if __name__ == '__main__':
    inspect()
