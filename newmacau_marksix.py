def _ensemble_strategy_v3_1(
    draws: List[List[int]],
    mined_config: Optional[Dict[str, float]],
    strategy_weights: Dict[str, float],
    conn: sqlite3.Connection,
    issue_no: str
) -> Tuple[List[Tuple[int, int, float, str]], int, float, Dict[int, float]]:
    sub_strategies = ["hot_v1", "cold_rebound_v1", "momentum_v1", "balanced_v1", "pattern_mined_v1"]
    score_maps = []
    sub_picks = {}
    
    bias_score, _ = detect_bias(conn, window=10)
    adjusted_weights = adjust_weights_for_bias(strategy_weights, bias_score)
    
    if bias_score > BIAS_THRESHOLD:
        print(f"[集成策略] 🔥 偏态模式激活，偏态系数={bias_score:.2f} 🔥", flush=True)
        cold_weight = adjusted_weights.get("cold_rebound_v1", 0.0)
        print(f"   → 冷号回补当前权重: {cold_weight:.3f}", flush=True)
    else:
        print(f"[集成策略] 正常模式，偏态系数={bias_score:.2f}", flush=True)

    for sub in sub_strategies:
        win_size = get_adaptive_strategy_window(sub, conn)
        sub_draws = draws[:win_size] if len(draws) > win_size else draws

        if sub == "pattern_mined_v1":
            cfg = mined_config or _default_mined_config()
            cfg["window"] = float(win_size)
            _, _, _, score_map = _apply_weight_config(sub_draws, cfg, "规律挖掘")
        else:
            config = {"window": float(win_size)}
            if sub == "hot_v1":
                config.update({"w_freq": 0.78, "w_omit": 0.05, "w_mom": 0.17})
            elif sub == "cold_rebound_v1":
                config.update({"w_freq": 0.05, "w_omit": 0.68, "w_mom": 0.27})
            elif sub == "momentum_v1":
                config.update({"w_freq": 0.12, "w_omit": 0.05, "w_mom": 0.83})
            else:
                config.update({"w_freq": 0.40, "w_omit": 0.30, "w_mom": 0.20})
            _, _, _, score_map = _apply_weight_config(sub_draws, config, STRATEGY_LABELS.get(sub, sub))

        score_maps.append(score_map)
        ranked = sorted(score_map.items(), key=lambda x: x[1], reverse=True)
        sub_picks[sub] = [n for n, _ in ranked[:6]]

    votes = {n: 0.0 for n in ALL_NUMBERS}
    for idx, sub in enumerate(sub_strategies):
        w = adjusted_weights.get(sub, 0.2)
        ranked = sorted(score_maps[idx].items(), key=lambda x: x[1], reverse=True)
        for rank, (n, _) in enumerate(ranked):
            votes[n] += w * (49 - rank)

    # === 新增：强化冷号回补策略的贡献 ===
    cold_picks = sub_picks.get("cold_rebound_v1", [])
    for idx, n in enumerate(cold_picks):
        votes[n] += 0.8 * (6 - idx)
    # ===================================

    for n in ALL_NUMBERS:
        appear = sum(1 for p in sub_picks.values() if n in p)
        votes[n] += (6 - appear) * ENSEMBLE_DIVERSITY_BONUS * 1.2

    voted = _normalize(votes)
    main_picked = _pick_top_six(voted, "集成投票v3.1")

    main6 = [n for n, _, _, _ in main_picked]
    special_number, confidence, _ = _generate_special_number_v4(conn, main6, issue_no)

    return main_picked, special_number, confidence, voted
