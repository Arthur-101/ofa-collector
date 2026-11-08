"""Self-check for the detector's per-flush cap. Run: python test_flush_cap.py"""

from detector import _cap_per_flush, MAX_SIGNALS_PER_RUN
import detector


def test_cap():
    rows = [{"signal_strength": float(i)} for i in range(MAX_SIGNALS_PER_RUN + 12)]
    out = _cap_per_flush(rows)
    assert len(out) == MAX_SIGNALS_PER_RUN, len(out)
    assert out[0]["signal_strength"] == float(MAX_SIGNALS_PER_RUN + 11)
    assert [r["signal_strength"] for r in out] == sorted(
        (r["signal_strength"] for r in out), reverse=True
    )
    # Under the cap nothing is dropped.
    small = [{"signal_strength": 1.0}, {"signal_strength": 2.0}]
    assert len(_cap_per_flush(small)) == 2
    print(f"flush cap ok: {len(out)} of {len(rows)} kept, strongest first")


def test_volume_spike_retired():
    assert not hasattr(detector, "_detect_volume_spike")
    print("VOLUME_SPIKE detector is gone")


if __name__ == "__main__":
    test_cap()
    test_volume_spike_retired()
