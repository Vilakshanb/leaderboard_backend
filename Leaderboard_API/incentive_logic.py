import os
from datetime import datetime

# --- Constants & Config ---
# Special-case regex (case-insensitive) for leader adjustments:
# Insurance slab boosted by INS leader points for Sumit C
INS_LEADER_EMP_REGEX = os.getenv(
    "PLI_INS_LEADER_EMP_REGEX", r"(?i)^sumit\s+c"
)  # e.g., "Sumit Ch..."
# MF tier boosted by INV leader points for Sagar M
MF_LEADER_EMP_REGEX = os.getenv("PLI_MF_LEADER_EMP_REGEX", r"(?i)^sagar\s+maini")

# Prefer Zoho employee-id based leader boosts (regex is fallback)
INS_LEADER_EMP_ID = os.getenv("PLI_INS_LEADER_EMP_ID")  # e.g., "2969103000154276001" (Sumit C)

MF_LEADER_EMP_ID = os.getenv("PLI_MF_LEADER_EMP_ID")  # e.g., "Sagar M"

# defaults (Moved from Leaderboard module to make this standalone)
default_thresholds = [
    {"tier": "T6", "min_val": 60000},
    {"tier": "T5", "min_val": 40000},
    {"tier": "T4", "min_val": 25000},
    {"tier": "T3", "min_val": 15000},
    {"tier": "T2", "min_val": 8000},
    {"tier": "T1", "min_val": 2000},
    {"tier": "T0", "min_val": -float('inf')},
]
default_factors = {
    "T6": 0.000037500,
    "T5": 0.000033333,
    "T4": 0.000029167,
    "T3": 0.000025000,
    "T2": 0.000020833,
    "T1": 0.000016667,
    "T0": 0.0,
}

def build_rupee_incentives_pipeline(month: str, start: datetime, end: datetime, sip_config: dict = None, ins_config: dict = None):
    """
    Build Rupee_Incentives from the already-written Public_Leaderboard.
    Ported from Leaderboard module to allow on-the-fly calculation in API.
    """

    # Defaults (Fallback if config missing)
    # Helpers to generate JS
    def make_js(thry_list, fact_dict):
        # Sort desc by min_val
        safe_thr = []
        if isinstance(thry_list, list):
            safe_thr = sorted(
                [dict(t, min_val=t.get("min_val", 0)) for t in thry_list],
                key=lambda x: x["min_val"],
                reverse=True
            )

        safe_fac = fact_dict if isinstance(fact_dict, dict) else {}

        # Tier JS
        t_js = "function(points) { "
        for t in safe_thr:
            tn = t.get("tier", "T0")
            mv = t.get("min_val", 0)
            if mv == -float('inf'):
                t_js += f"return '{tn}'; "
            else:
                t_js += f"if (points >= {mv}) return '{tn}'; "
        t_js += "return 'T0'; }"

        # Factor JS
        f_js = "function(tier) { switch(tier) { "
        for tc, r in safe_fac.items():
            f_js += f"case '{tc}': return {r}; "
        f_js += "default: return 0.0; } }"

        return t_js, f_js

    # Determine Mode
    scoring_mode = "unified"
    if sip_config and "scoring_mode" in sip_config:
        scoring_mode = sip_config["scoring_mode"]

    # Generate JS bodies
    if scoring_mode == "individual":
        # SIP
        sip_thr = sip_config.get("tier_thresholds", default_thresholds)
        sip_fac = sip_config.get("tier_factors", default_factors)
        sip_tier_js, sip_factor_js = make_js(sip_thr, sip_fac)

        # Lump
        lump_thr = sip_config.get("lumpsum_tier_thresholds", default_thresholds)
        lump_fac = sip_config.get("lumpsum_tier_factors", default_factors)
        lump_tier_js, lump_factor_js = make_js(lump_thr, lump_fac)

        # Unified (Fallback/Informational) - use SIP config? or just defaults?
        # For 'individual' mode, 'mf_points_effective' logic is ambiguous
        # but let's just use SIP logic to avoid errors if referenced
        unified_tier_js, unified_factor_js = sip_tier_js, sip_factor_js

    else:
        # Unified
        uni_thr = default_thresholds
        uni_fac = default_factors
        if sip_config:
            uni_thr = sip_config.get("tier_thresholds", default_thresholds)
            uni_fac = sip_config.get("tier_factors", default_factors)

        unified_tier_js, unified_factor_js = make_js(uni_thr, uni_fac)
        sip_tier_js, sip_factor_js = unified_tier_js, unified_factor_js
        lump_tier_js, lump_factor_js = unified_tier_js, unified_factor_js

    # Logic for MF Rupees
    # Unified: AUM * Factor
    # Individual: SIP_Rupees + Lump_Rupees
    mf_rupees_expr = {"$round": [{"$multiply": ["$aum_for_calc", "$mf_factor"]}, 2]}

    if scoring_mode == "individual":
        mf_rupees_expr = {
            "$add": ["$mf_sip_rupees", "$mf_lump_rupees"]
        }



    # ---- Insurance Logic Generation ----
    # Default Slabs (Fallback) matching hardcoded logic
    default_ins_slabs = [
         {"min_points": 0, "max_points": 500, "fresh_pct": 0.0, "renew_pct": 0.0, "bonus_rupees": 0, "label": "<500"},
         {"min_points": 500, "max_points": 1000, "fresh_pct": 0.0050, "renew_pct": 0.0, "bonus_rupees": 0, "label": "500–999"},
         {"min_points": 1000, "max_points": 1500, "fresh_pct": 0.0100, "renew_pct": 0.0020, "bonus_rupees": 0, "label": "1000–1499"},
         {"min_points": 1500, "max_points": 2000, "fresh_pct": 0.0125, "renew_pct": 0.0040, "bonus_rupees": 0, "label": "1500–1999"},
         {"min_points": 2000, "max_points": 2500, "fresh_pct": 0.0150, "renew_pct": 0.0050, "bonus_rupees": 0, "label": "2000–2499"},
         {"min_points": 2500, "max_points": None, "fresh_pct": 0.0175, "renew_pct": 0.0075, "bonus_rupees": 2000, "label": "2500+"},
    ]

    use_ins_slabs = default_ins_slabs
    if ins_config and "slabs" in ins_config:
        use_ins_slabs = ins_config["slabs"]

    # Sort ASC by min_points
    use_ins_slabs.sort(key=lambda x: x.get("min_points", 0))

    # Build branches
    # Strategy: Use $switch with $lt checks. Since switch stops at first match,
    # ordering lowest-max first works.
    # The last slab (open-ended) becomes the 'default'.

    # Build branches helper
    def _gen_branches(slabs):
        lbl_br, fr_br, ren_br, bon_br = [], [], [], []
        lbl_d, fr_d, ren_d, bon_d = "<500", 0.0, 0.0, 0

        # Sort ASC
        sorted_slabs = sorted(slabs, key=lambda x: x.get("min_points", 0))

        for s in sorted_slabs:
            mx = s.get("max_points")
            if mx is None:
                lbl_d = s.get("label", "")
                fr_d = s.get("fresh_pct", 0.0)
                ren_d = s.get("renew_pct", 0.0)
                bon_d = s.get("bonus_rupees", 0)
            else:
                cond = {"$lt": ["$ins_points_effective", mx]}
                lbl_br.append({"case": cond, "then": s.get("label", "")})
                fr_br.append({"case": cond, "then": s.get("fresh_pct", 0.0)})
                ren_br.append({"case": cond, "then": s.get("renew_pct", 0.0)})
                bon_br.append({"case": cond, "then": s.get("bonus_rupees", 0)})
        return (lbl_br, lbl_d), (fr_br, fr_d), (ren_br, ren_d), (bon_br, bon_d)

    # 1. Insurance RM Slabs
    (ins_lbl_br, ins_lbl_def), (ins_fr_br, ins_fr_def), (ins_ren_br, ins_ren_def), (ins_bon_br, ins_bon_def) = _gen_branches(use_ins_slabs)

    # 2. Investment RM Slabs (Fallback to INS logic if missing)
    use_inv_slabs = use_ins_slabs
    if ins_config and ins_config.get("slabs_investment_rm"):
        use_inv_slabs = ins_config["slabs_investment_rm"]

    (inv_lbl_br, inv_lbl_def), (inv_fr_br, inv_fr_def), (inv_ren_br, inv_ren_def), (inv_bon_br, inv_bon_def) = _gen_branches(use_inv_slabs)

    return [
        # Base spine: one row per RM from the public leaderboard for the month
        {
            "$match": {
                "period_month": month,
            }
        },
        {
            "$project": {
                "period_month": 1,
                "rm_name": 1,
                "employee_id": {"$toString": "$employee_id"},
                "is_active_public": {"$ifNull": ["$is_active", True]},
                "mf_points": {"$ifNull": ["$mf_points", 0]},
                "mf_sip_points": {"$ifNull": ["$mf_sip_points", 0]},
                "mf_lumpsum_points": {"$ifNull": ["$mf_lumpsum_points", 0]},
                "ins_points": {"$ifNull": ["$ins_points", 0]},
                "ref_points": {"$ifNull": ["$ref_points", 0]},
                # aum_first will be brought from MF_SIP_Leaderboard
                "aum_first": {"$literal": 0.0},
            }
        },
        # Lookup Zoho Profile to determine if Investment RM
        {
            "$lookup": {
                "from": "Zoho_Users",
                "localField": "employee_id",
                "foreignField": "Employee_ID",
                "as": "_zoho_user"
            }
        },
        {
             "$addFields": {
                 "is_inv_rm": {
                      "$regexMatch": {
                          "input": {"$ifNull": [{"$first": "$_zoho_user.Profile"}, ""]},
                          "regex": "Mutual Funds",
                          "options": "i"
                      }
                 }
             }
        },
        # Bring AUM for MF payout: best-effort from MF_SIP_Leaderboard for that month/employee
        {
            "$lookup": {
                "from": "MF_SIP_Leaderboard",
                "let": {"emp": "$employee_id", "m": "$period_month"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$ifNull": [
                                                    "$period_month",
                                                    {"$ifNull": ["$month", "$$m"]},
                                                ]
                                            },
                                            "$$m",
                                        ]
                                    },
                                    {"$eq": [{"$toString": "$employee_id"}, "$$emp"]},
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "aum_first": {
                                "$let": {
                                    "vars": {"v": {"$toDouble": {"$ifNull": ["$aum_start", 0]}}},
                                    "in": {
                                        "$cond": [
                                            {"$eq": ["$$v", "$$v"]},
                                            "$$v",
                                            0.0,
                                        ]
                                    },
                                }
                            },
                        }
                    },
                ],
                "as": "sip_aum_col",
            }
        },
        # Bring Lumpsum AUM from Leaderboard_Lumpsum
        {
            "$lookup": {
                "from": "Leaderboard_Lumpsum",
                "let": {"emp": "$employee_id", "m": "$period_month"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$month", "$$m"]},
                                    {"$eq": [{"$toString": "$employee_id"}, "$$emp"]},
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "lump_aum": {
                                "$let": {
                                    "vars": {"v": {"$toDouble": {"$ifNull": ["$AUM (Start of Month)", 0]}}},
                                    "in": {
                                        "$cond": [
                                            {"$eq": ["$$v", "$$v"]},
                                            "$$v",
                                            0.0,
                                        ]
                                    },
                                }
                            },
                        }
                    },
                ],
                "as": "lump_aum_col",
            }
        },
        {
            "$addFields": {
                "aum_first": {
                    "$ifNull": [
                        {"$max": "$sip_aum_col.aum_first"},
                        0.0,
                    ]
                },
                "lump_aum_raw": {
                    "$ifNull": [
                        {"$max": "$lump_aum_col.lump_aum"},
                        0.0,
                    ]
                }
            }
        },
        {
             "$addFields": {
                 "sip_aum_derived": {
                     "$max": [0.0, {"$subtract": ["$aum_first", "$lump_aum_raw"]}]
                 }
             }
        },
        # Bring leader bonuses (INS & INV) for the month
        {
            "$lookup": {
                "from": "MF_Leaders",
                "let": {"rm": "$rm_name", "m": "$period_month"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$period_month", "$$m"]},
                                    {"$eq": ["$rm_name", "$$rm"]},
                                    {"$in": ["$bucket", ["INS", "MF"]]},
                                ]
                            }
                        },
                    },
                    {"$project": {"_id": 0, "bucket": 1, "leader_bonus_points": 1}},
                ],
                "as": "leaders",
            }
        },
        {
            "$addFields": {
                "leader_ins_points": {
                    "$ifNull": [
                        {
                            "$first": {
                                "$map": {
                                    "input": {
                                        "$filter": {
                                            "input": "$leaders",
                                            "as": "l",
                                            "cond": {"$eq": ["$$l.bucket", "INS"]},
                                        }
                                    },
                                    "as": "x",
                                    "in": "$$x.leader_bonus_points",
                                }
                            }
                        },
                        0,
                    ]
                },
                "leader_inv_points": {
                    "$ifNull": [
                        {
                            "$first": {
                                "$map": {
                                    "input": {
                                        "$filter": {
                                            "input": "$leaders",
                                            "as": "l",
                                            "cond": {"$eq": ["$$l.bucket", "INV"]},
                                        }
                                    },
                                    "as": "x",
                                    "in": "$$x.leader_bonus_points",
                                }
                            }
                        },
                        0,
                    ]
                },
            }
        },
        {
            "$addFields": {
                "rm_lower": {"$toLower": {"$ifNull": ["$rm_name", ""]}},
                # Base MF points = mf_points
                "mf_points_base": {"$ifNull": ["$mf_points", 0]},
                "is_ins_leader_empid": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": [INS_LEADER_EMP_ID, None]},
                                {"$ne": [INS_LEADER_EMP_ID, ""]},
                                {"$eq": ["$employee_id", INS_LEADER_EMP_ID]},
                            ]
                        },
                        True,
                        False,
                    ]
                },
                "is_mf_leader_empid": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": [MF_LEADER_EMP_ID, None]},
                                {"$ne": [MF_LEADER_EMP_ID, ""]},
                                {"$eq": ["$employee_id", MF_LEADER_EMP_ID]},
                            ]
                        },
                        True,
                        False,
                    ]
                },
            }
        },
        # Apply leader adjustments (ID-based, with regex fallback)
        {
            "$addFields": {
                "ins_points_effective": {
                    "$add": [
                        "$ins_points",
                        {
                            "$cond": [
                                {
                                    "$or": [
                                        "$is_ins_leader_empid",
                                        {
                                            "$regexMatch": {
                                                "input": "$rm_lower",
                                                "regex": INS_LEADER_EMP_REGEX,
                                            }
                                        },
                                    ]
                                },
                                {"$ifNull": ["$leader_ins_points", 0]},
                                0,
                            ]
                        },
                    ]
                },
                "mf_points_effective": {
                    "$add": [
                        "$mf_points_base",
                        {
                            "$cond": [
                                {
                                    "$or": [
                                        "$is_mf_leader_empid",
                                        {
                                            "$regexMatch": {
                                                "input": "$rm_lower",
                                                "regex": MF_LEADER_EMP_REGEX,
                                            }
                                        },
                                    ]
                                },
                                {"$ifNull": ["$leader_inv_points", 0]},
                                0,
                            ]
                        },
                    ]
                },
            }
        },
        {
            "$addFields": {
                "ins_slab_label": {
                    "$cond": [
                        {"$eq": ["$is_inv_rm", True]},
                        {"$switch": {"branches": inv_lbl_br, "default": inv_lbl_def}},
                        {"$switch": {"branches": ins_lbl_br, "default": ins_lbl_def}},
                    ]
                },
                "ins_fresh_pct": {
                    "$cond": [
                        {"$eq": ["$is_inv_rm", True]},
                        {"$switch": {"branches": inv_fr_br, "default": inv_fr_def}},
                        {"$switch": {"branches": ins_fr_br, "default": ins_fr_def}},
                    ]
                },
                "ins_renew_pct": {
                    "$cond": [
                        {"$eq": ["$is_inv_rm", True]},
                        {"$switch": {"branches": inv_ren_br, "default": inv_ren_def}},
                        {"$switch": {"branches": ins_ren_br, "default": ins_ren_def}},
                    ]
                },
                "ins_bonus_rupees": {
                    "$cond": [
                        {"$eq": ["$is_inv_rm", True]},
                        {"$switch": {"branches": inv_bon_br, "default": inv_bon_def}},
                        {"$switch": {"branches": ins_bon_br, "default": ins_bon_def}},
                    ]
                },
            }
        },
        # Gather monthly fresh/renew premium from Insurance_Policy_Scoring (best-effort schema)
        {
            "$lookup": {
                "from": "Insurance_Policy_Scoring",
                "let": {"emp": "$employee_id"},
                "pipeline": [
                    {
                        "$match": {
                            "conversion_date": {"$gte": start, "$lt": end},
                        }
                    },
                    {
                        "$match": {
                            "$expr": {"$eq": [{"$toString": "$employee_id"}, "$$emp"]},
                        }
                    },
                    {
                        "$project": {
                            "this_year_premium": {
                                "$toDouble": {"$ifNull": ["$this_year_premium", 0]}
                            },
                            "renewal_notice_premium": {
                                "$toDouble": {"$ifNull": ["$renewal_notice_premium", 0]}
                            },
                            "last_year_premium": {
                                "$toDouble": {"$ifNull": ["$last_year_premium", 0]}
                            },
                            "policy_classification": {
                                "$toLower": {"$ifNull": ["$policy_classification", ""]}
                            },
                            "conversion_status": {
                                "$toLower": {"$ifNull": ["$conversion_status", ""]}
                            },
                        }
                    },
                    {
                        "$addFields": {
                            "renew_flag": {
                                "$or": [
                                    {"$in": ["$policy_classification", ["renewal", "renew"]]},
                                    {
                                        "$regexMatch": {
                                            "input": "$conversion_status",
                                            "regex": "renew",
                                        }
                                    },
                                ]
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "fresh_prem": {
                                "$sum": {
                                    "$cond": [
                                        {"$eq": ["$renew_flag", False]},
                                        {"$ifNull": ["$this_year_premium", 0]},
                                        0,
                                    ]
                                }
                            },
                            "renew_prem": {
                                "$sum": {
                                    "$cond": [
                                        {"$eq": ["$renew_flag", True]},
                                        {
                                            "$ifNull": [
                                                {
                                                    "$ifNull": [
                                                        "$renewal_notice_premium",
                                                        "$last_year_premium",
                                                    ]
                                                },
                                                0,
                                            ]
                                        },
                                        0,
                                    ]
                                }
                            },
                        }
                    },
                ],
                "as": "prem",
            }
        },
        {
            "$addFields": {
                "fresh_premium": {"$ifNull": [{"$first": "$prem.fresh_prem"}, 0]},
                "renew_premium": {"$ifNull": [{"$first": "$prem.renew_prem"}, 0]},
            }
        },
        {
            "$addFields": {
                "ins_rupees_from_fresh": {
                    "$round": [{"$multiply": ["$ins_fresh_pct", "$fresh_premium"]}, 2]
                },
                "ins_rupees_from_renew": {
                    "$round": [{"$multiply": ["$ins_renew_pct", "$renew_premium"]}, 2]
                },
                "ins_rupees_total": {
                    "$add": [
                        "$ins_bonus_rupees",
                        {"$round": [{"$multiply": ["$ins_fresh_pct", "$fresh_premium"]}, 2]},
                        {"$round": [{"$multiply": ["$ins_renew_pct", "$renew_premium"]}, 2]},
                    ]
                },
            }
        },
        # ---- Mutual Fund tier & payout (Combined & Split) ----
        {
            "$addFields": {
                # Helper macro for Tier Calculation
                # Standard Tiers
                "mf_tier_calc": {
                    "$function": {
                        "body": unified_tier_js,
                        "args": ["$mf_points_effective"],
                        "lang": "js"
                    }
                },
                 "mf_tier_sip_calc": {
                    "$function": {
                        "body": sip_tier_js,
                        "args": ["$mf_sip_points"],
                        "lang": "js"
                    }
                },
                 "mf_tier_lump_calc": {
                    "$function": {
                        "body": lump_tier_js,
                        "args": ["$mf_lumpsum_points"],
                        "lang": "js"
                    }
                }
            }
        },
        {
            "$addFields": {
                "mf_tier": "$mf_tier_calc",
                "mf_sip_tier": "$mf_tier_sip_calc",
                "mf_lump_tier": "$mf_tier_lump_calc"
            }
        },
        # Helper: Factor from Tier
        {
            "$addFields": {
               "factor_lookup": {
                    "$function": {
                        "body": unified_factor_js,
                        "args": ["$mf_tier"],
                        "lang": "js"
                    }
               },
               "factor_sip_lookup": {
                    "$function": {
                        "body": sip_factor_js,
                        "args": ["$mf_sip_tier"],
                        "lang": "js"
                    }
               },
               "factor_lump_lookup": {
                    "$function": {
                        "body": lump_factor_js,
                        "args": ["$mf_lump_tier"],
                        "lang": "js"
                    }
               }
            }
        },
        {
            "$addFields": {
                "mf_factor": "$factor_lookup",
                "mf_sip_factor": "$factor_sip_lookup",
                "mf_lump_factor": "$factor_lump_lookup",
                "aum_for_calc": {
                    "$let": {
                        "vars": {"v": {"$toDouble": {"$ifNull": ["$aum_first", 0]}}},
                        "in": {
                            "$cond": [
                                {"$eq": ["$$v", "$$v"]},
                                "$$v",
                                0.0,
                            ]
                        },
                    }
                },
            }
        },
        {
            "$addFields": {
                "mf_sip_rupees": {"$round": [{"$multiply": ["$sip_aum_derived", "$mf_sip_factor"]}, 2]},
                "mf_lump_rupees": {"$round": [{"$multiply": ["$lump_aum_raw", "$mf_lump_factor"]}, 2]},
            }
        },
        {
            "$addFields": {
                 # mf_rupees calculation depends on scoring mode
                 "mf_rupees": mf_rupees_expr
            }
        },
        # is_active & 6-month eligibility from Zoho_Users
        {
            "$lookup": {
                "from": "Zoho_Users",
                "let": {"emp": "$employee_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": [{"$toString": "$id"}, "$$emp"]}}},
                    {
                        "$project": {
                            "_id": 0,
                            "status": "$status",
                            "Status": "$Status",
                            "active": "$active",
                            "is_active": "$is_active",
                            "IsActive": "$IsActive",
                            "inactive_since": "$inactive_since",
                            "employee_id": "$employee_id",
                            "Employee ID": "$Employee ID",
                            "full": "$Full Name",
                            "alt": "$Name",
                        }
                    },
                ],
                "as": "zu",
            }
        },
        {
            "$addFields": {
                "has_zoho_user": {"$gt": [{"$size": "$zu"}, 0]},
                "is_active": {
                    "$let": {
                        "vars": {
                            "st": {
                                "$toLower": {
                                    "$ifNull": [
                                        {"$first": "$zu.status"},
                                        {"$first": "$zu.Status"},
                                        "",
                                    ]
                                }
                            },
                            "a1": {"$first": "$zu.active"},
                            "a2": {"$first": "$zu.is_active"},
                            "a3": {"$first": "$zu.IsActive"},
                        },
                        "in": {
                            "$or": [
                                {"$eq": ["$$st", "active"]},
                                {"$eq": ["$$a1", True]},
                                {"$eq": ["$$a2", True]},
                                {"$eq": ["$$a3", True]},
                            ]
                        },
                    }
                },
                "skip_by_inactive_no_empid": {
                    "$let": {
                        "vars": {
                            "st": {
                                "$toLower": {
                                    "$ifNull": [
                                        {"$first": "$zu.status"},
                                        {"$first": "$zu.Status"},
                                        "",
                                    ]
                                }
                            },
                            "empid": {
                                "$ifNull": [
                                    {"$first": "$zu.employee_id"},
                                    {"$first": "$zu.Employee ID"},
                                    "",
                                ]
                            },
                        },
                        "in": {
                            "$and": [
                                {"$eq": ["$$st", "inactive"]},
                                {
                                    "$eq": [
                                        {"$trim": {"input": {"$toString": "$$empid"}}},
                                        "",
                                    ]
                                },
                            ]
                        },
                    }
                },
                "inactive_since_raw": {"$first": "$zu.inactive_since"},
                "rm_name_final": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": ["$rm_name", None]},
                                {"$ne": ["$rm_name", ""]},
                            ]
                        },
                        "$rm_name",
                        {
                            "$let": {
                                "vars": {"z": {"$first": "$zu"}},
                                "in": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$ne": ["$$z.full", None]},
                                                {"$ne": ["$$z.full", ""]},
                                            ]
                                        },
                                        "$$z.full",
                                        {
                                            "$cond": [
                                                {
                                                    "$and": [
                                                        {"$ne": ["$$z.alt", None]},
                                                        {"$ne": ["$$z.alt", ""]},
                                                    ]
                                                },
                                                "$$z.alt",
                                                {
                                                    "$concat": [
                                                        "Unmapped-",
                                                        {"$toString": "$employee_id"},
                                                    ]
                                                },
                                            ]
                                        },
                                    ]
                                },
                            }
                        },
                    ]
                },
                "period_date": {
                    "$dateFromString": {
                        "dateString": {"$concat": ["$period_month", "-01"]},
                        "format": "%Y-%m-%d",
                    }
                },
                "inactive_until": {
                    "$cond": [
                        {"$ne": ["$inactive_since_raw", None]},
                        {
                            "$dateAdd": {
                                "startDate": "$inactive_since_raw",
                                "unit": "month",
                                "amount": 6,
                            }
                        },
                        None,
                    ]
                },
                "eligible_by_inactive": {
                    "$cond": [
                        {
                            "$or": [
                                "$is_active",
                                {"$eq": ["$inactive_since_raw", None]},
                            ]
                        },
                        True,
                        {
                            "$and": [
                                {"$gte": ["$period_date", "$inactive_since_raw"]},
                                {"$lt": ["$period_date", "$inactive_until"]},
                            ]
                        },
                    ]
                },
            }
        },
        # ---- Referral Logic ----
        {
            "$addFields": {
                "ref_rupees": {
                    "$cond": [
                        {"$gte": ["$ref_points", 1]},
                        {"$multiply": ["$ref_points", 250]},
                        0,
                    ]
                }
            }
        },
        # ---- Final Total ----
        {
            "$addFields": {
                "total_incentive": {
                    "$add": [
                        {"$ifNull": ["$ins_rupees_total", 0]},
                        {"$ifNull": ["$mf_rupees", 0]},
                        {"$ifNull": ["$ref_rupees", 0]},
                    ]
                },
                "audit": {
                    "tier": "$mf_tier",
                    "rate": "$mf_factor",
                    "ins_slab": "$ins_slab_label",
                    "unified_logic": True
                }
            }
        },
        {
            "$match": {
                "rm_name_final": {"$nin": [None, ""]},
                # "has_zoho_user": True,
                # Hard filter: Zoho user is inactive AND has no employee_id → skip from Rupee_Incentives
                "skip_by_inactive_no_empid": {"$ne": True},
            }
        },
        # Final doc with audit trail
        {
            "$project": {
                "_id": 0,
                "period_month": 1,
                "rm_name": "$rm_name_final",
                "employee_id": 1,
                "is_active": {"$ifNull": ["$is_active", True]},
                "mf_points": 1,
                "mf_sip_points": 1,
                "mf_lumpsum_points": 1,
                "ins_points": 1,
                "ref_points": 1,
                "leader_ins_points": 1,
                "leader_inv_points": 1,
                "ins_points_effective": 1,
                "mf_points_effective": 1,
                "ins_slab_label": 1,
                "ins_fresh_pct": 1,
                "ins_renew_pct": 1,
                "ins_bonus_rupees": 1,
                "fresh_premium": 1,
                "renew_premium": 1,
                "ins_rupees_from_fresh": 1,
                "ins_rupees_from_renew": 1,
                "ins_rupees_total": 1,
                "ref_rupees": 1,
                "total_incentive": 1,
                "audit": 1,
                "aum_first": "$aum_for_calc",
                "aum_lumpsum": "$lump_aum_raw",
                "aum_sip": "$sip_aum_derived",
                "mf_tier": 1,
                "mf_factor": 1,
                "mf_rupees": 1,
                "mf_sip_tier": 1,
                "mf_sip_factor": 1,
                "mf_sip_rupees": 1,
                "mf_lump_tier": 1,
                "mf_lump_factor": 1,
                "mf_lump_rupees": 1,
                "eligible_by_inactive": 1,
            }
        },
    ]
