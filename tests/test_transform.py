from chess_lakehouse.transform import (
    aggregate_gold,
    normalize_game,
    parse_pgn_games,
    stable_game_id,
)

VALID_PGN = """[Event "Rated Blitz game"]
[Site "https://lichess.org/alpha001"]
[UTCDate "2026.01.02"]
[UTCTime "10:00:00"]
[White "alice"]
[Black "bob"]
[Result "1-0"]
[WhiteElo "1500"]
[BlackElo "1700"]
[TimeControl "300+0"]
[Opening "Sicilian Defense"]

1. e4 c5 2. Nf3 d6 1-0
"""


def test_parser_yields_complete_pgn_blocks():
    blocks = list(parse_pgn_games(VALID_PGN + "\n" + VALID_PGN.replace("alpha001", "beta0002")))

    assert len(blocks) == 2
    assert blocks[0].startswith('[Event "Rated Blitz game"]')
    assert blocks[1].endswith("1-0")


def test_stable_id_uses_lichess_game_identifier_across_duplicate_deliveries():
    changed_delivery = VALID_PGN.replace("Rated Blitz game", "Re-exported event")

    assert stable_game_id(VALID_PGN) == "alpha001"
    assert stable_game_id(changed_delivery) == "alpha001"


def test_normalize_game_quarantines_malformed_required_fields():
    malformed = VALID_PGN.replace('[WhiteElo "1500"]', '[WhiteElo "unknown"]')

    normalized, errors = normalize_game(malformed)

    assert normalized is None
    assert errors == ["white_elo must be an integer"]


def test_normalize_game_derives_analytics_dimensions():
    normalized, errors = normalize_game(VALID_PGN)

    assert errors == []
    assert normalized is not None
    assert normalized["game_id"] == "alpha001"
    assert normalized["played_at"] == "2026-01-02T10:00:00Z"
    assert normalized["rating_bucket"] == "1600-1799"
    assert normalized["speed"] == "blitz"
    assert normalized["opening"] == "Sicilian Defense"


def test_gold_metrics_preserve_game_count_across_each_dimension():
    first, _ = normalize_game(VALID_PGN)
    second, _ = normalize_game(
        VALID_PGN.replace("alpha001", "beta0002")
        .replace('[Result "1-0"]', '[Result "0-1"]')
        .replace('[TimeControl "300+0"]', '[TimeControl "900+10"]')
        .replace('[Opening "Sicilian Defense"]', '[Opening "Queen\'s Gambit"]')
    )

    metrics = aggregate_gold([first, second])

    assert sum(row["games"] for row in metrics["opening_metrics"]) == 2
    assert sum(row["games"] for row in metrics["rating_metrics"]) == 2
    assert sum(row["games"] for row in metrics["time_control_metrics"]) == 2
    assert metrics["time_control_metrics"] == [
        {
            "speed": "blitz",
            "games": 1,
            "white_wins": 1,
            "draws": 0,
            "white_win_rate": 1.0,
        },
        {
            "speed": "rapid",
            "games": 1,
            "white_wins": 0,
            "draws": 0,
            "white_win_rate": 0.0,
        },
    ]
