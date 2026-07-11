from coincall_rfq_maker.orchestration import TransientOutageGate


def test_record_transient_uses_one_second_base_delay_on_first_failure() -> None:
    gate = TransientOutageGate()

    gate.record_transient(0)

    assert gate.cooldown_until_ms == 1_000


def test_record_transient_doubles_base_delay_for_each_failure() -> None:
    for consecutive_failures, expected_cooldown in (
        (0, 1_000),
        (1, 2_000),
        (2, 4_000),
        (3, 8_000),
        (4, 16_000),
        (5, 32_000),
    ):
        gate = TransientOutageGate(
            consecutive_failures=consecutive_failures,
            jitter_counter=0,
        )

        gate.record_transient(0)

        assert gate.cooldown_until_ms == expected_cooldown


def test_record_transient_caps_base_delay_at_sixty_seconds() -> None:
    for consecutive_failures in (6, 7, 12):
        gate = TransientOutageGate(
            consecutive_failures=consecutive_failures,
            jitter_counter=0,
        )

        gate.record_transient(0)

        assert gate.cooldown_until_ms == 60_000


def test_record_transient_adds_jitter_on_top_of_capped_base_delay() -> None:
    # The 60_000 ms cap applies to the BASE delay only; jitter is added on top of the
    # capped base. An implementation that capped the combined (base + jitter) total, or
    # dropped jitter once saturated, would still pass every jitter_counter=0 cap case —
    # so pin the interaction with an explicit full-cooldown expectation.
    for consecutive_failures, jitter_counter, expected_cooldown in (
        (6, 1, 60_050),
        (6, 2, 60_100),
        (7, 3, 60_150),
        (12, 4, 60_200),
    ):
        gate = TransientOutageGate(
            consecutive_failures=consecutive_failures,
            jitter_counter=jitter_counter,
        )

        gate.record_transient(0)

        assert gate.cooldown_until_ms == expected_cooldown


def test_record_transient_cycles_jitter_every_five_steps() -> None:
    for jitter_counter, expected_cooldown in (
        (0, 1_000),
        (1, 1_050),
        (2, 1_100),
        (3, 1_150),
        (4, 1_200),
        (5, 1_000),
        (6, 1_050),
    ):
        gate = TransientOutageGate(
            consecutive_failures=0,
            jitter_counter=jitter_counter,
        )

        gate.record_transient(0)

        assert gate.cooldown_until_ms == expected_cooldown


def test_record_transient_adds_delay_to_now_ms() -> None:
    gate = TransientOutageGate(consecutive_failures=0, jitter_counter=0)

    gate.record_transient(10_000)

    assert gate.cooldown_until_ms == 11_000


def test_in_cooldown_uses_strict_deadline_boundary() -> None:
    gate = TransientOutageGate(cooldown_until_ms=5_000)

    assert gate.in_cooldown(4_999) is True
    assert gate.in_cooldown(5_000) is False
    assert gate.in_cooldown(5_001) is False


def test_record_success_resets_cooldown_and_failures_but_keeps_jitter_counter() -> None:
    gate = TransientOutageGate(
        cooldown_until_ms=12_345,
        consecutive_failures=3,
        jitter_counter=4,
    )

    gate.record_success()

    assert gate.cooldown_until_ms == 0
    assert gate.consecutive_failures == 0
    assert gate.jitter_counter == 4


def test_record_transient_increments_both_counters_each_time() -> None:
    gate = TransientOutageGate()

    gate.record_transient(0)
    gate.record_transient(0)
    gate.record_transient(0)

    assert gate.consecutive_failures == 3
    assert gate.jitter_counter == 3
