import db_supabase
import backfill_trades

def test_bias():
    c = db_supabase.get_supabase()
    for sig_id in [75, 124, 89, 128, 142]:
        sig = c.table('signals').select('*').eq('id', sig_id).execute().data[0]
        stype, bias = backfill_trades._rederive_quadrant_bias(sig, c)
        print(f"Signal {sig_id}: Old bias={sig['bias']} -> Quadrant bias={bias} (type: {stype})")

if __name__ == '__main__':
    test_bias()
