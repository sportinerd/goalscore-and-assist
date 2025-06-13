import json
import pandas as pd
import numpy as np
import os
import io
import csv
import re
from datetime import datetime as datetime_cls, timedelta, date as date_cls # Modified import
from scipy.stats import poisson
from bs4 import BeautifulSoup
from typing import List, Dict, Any, FrozenSet, Optional, Tuple

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

# --- Configuration: Main Data Directory ---
DATA_DIR = 'data' # All data files should be in a 'data' subdirectory

# Create data directory if it doesn't exist (useful for local testing)
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
    print(f"INFO: Created data directory: {DATA_DIR}")
    print(f"IMPORTANT: Please place all required data files (JSON, XLSX, HTML, MD) into the '{DATA_DIR}' directory.")


# --- File Paths (using DATA_DIR) ---
CORRECT_SCORE_FILE_PATH = os.path.join(DATA_DIR, "correct_score.json")
PLAYER_STATS_FP = os.path.join(DATA_DIR, 'merged_mapped_players.xlsx')
HTML_ODDS_FP = os.path.join(DATA_DIR, 'fifa_club_wc_odds.html')
MD_ODDS_FP = os.path.join(DATA_DIR, 'fifa_club_wc_odds.md')
UNIFIED_ANYTIME_GOALSCORER_FILE_PATH = os.path.join(DATA_DIR, "updated_anytimegoalscorer.json")
OUTPUT_COMBINED_PLAYER_STATS_JSON_FP = os.path.join(DATA_DIR, 'player_combined_match_stats_output.json')


# --- Team Name Mapping ---
TEAM_NAME_MAPPING = {
    "Real Madrid": "Real Madrid CF", "Manchester City": "Manchester City FC", "Man City": "Manchester City FC",
    "Bayern Munich": "FC Bayern München", "Bayern": "FC Bayern München",
    "PSG": "Paris Saint-Germain", "Paris SG": "Paris Saint-Germain", "Paris Saint Germain": "Paris Saint-Germain",
    "Inter": "FC Internazionale Milano", "Inter Milan": "FC Internazionale Milano",
    "Chelsea": "Chelsea FC",
    "Atl. Madrid": "Atlético de Madrid", "Atletico Madrid": "Atlético de Madrid", "Atl Madrid": "Atlético de Madrid",
    "Dortmund": "Borussia Dortmund",
    "Juventus": "Juventus FC",
    "FC Porto": "FC Porto", "Porto": "FC Porto",
    "Flamengo RJ": "CR Flamengo", "Flamengo": "CR Flamengo", "Regatas Flamengo RJ": "CR Flamengo",
    "Benfica": "SL Benfica",
    "Palmeiras": "SE Palmeiras", "Sociedade Esportiva Palmeiras": "SE Palmeiras",
    "Boca Juniors": "CA Boca Juniors",
    "River Plate": "CA River Plate", "CA River Plate BA": "CA River Plate",
    "Botafogo RJ": "Botafogo FR", "Botafogo": "Botafogo FR", "Botafogo de Futebol e Regatas": "Botafogo FR",
    "Fluminense": "Fluminense FC", "Fluminense Football Club": "Fluminense FC",
    "Al Hilal": "Al Hilal SFC", "Al Hilal Riyadh": "Al Hilal SFC", "Al-Hilal": "Al Hilal SFC",
    "Inter Miami": "Inter Miami CF",
    "Salzburg": "FC Salzburg", "Red Bull Salzburg": "FC Salzburg",
    "Los Angeles FC": "LAFC",
    "Seattle Sounders": "Seattle Sounders FC",
    "Al Ahly": "Al Ahly FC", "Al Ahly SC": "Al Ahly FC",
    "Pachuca": "CF Pachuca",
    "Urawa Reds": "Urawa Red Diamonds",
    "Ulsan Hyundai": "Ulsan HD FC", "Ulsan HD": "Ulsan HD FC", "Ulsan Hyundai FC": "Ulsan HD FC",
    "Al Ain": "Al Ain FC", "Al Ain Abu Dhabi": "Al Ain FC",
    "Monterrey": "CF Monterrey",
    "Esperance Tunis": "Espérance Sportive de Tunis", "ES Tunis": "Espérance Sportive de Tunis",
    "Wydad Athletic": "Wydad AC", "Wydad Casablanca": "Wydad AC", "Wydad AC Casablanca": "Wydad AC",
    "Mamelodi Sundowns": "Mamelodi Sundowns FC",
    "Auckland City": "Auckland City FC",
    "Real Madrid CF": "Real Madrid CF", "Manchester City FC": "Manchester City FC",
    "FC Bayern München": "FC Bayern München", "Paris Saint-Germain": "Paris Saint-Germain",
    "FC Internazionale Milano": "FC Internazionale Milano", "Chelsea FC": "Chelsea FC",
    "Atlético de Madrid": "Atlético de Madrid", "Borussia Dortmund": "Borussia Dortmund",
    "Juventus FC": "Juventus FC", "CR Flamengo": "CR Flamengo",
    "SL Benfica": "SL Benfica", "SE Palmeiras": "SE Palmeiras",
    "CA Boca Juniors": "CA Boca Juniors", "CA River Plate": "CA River Plate",
    "Botafogo FR": "Botafogo FR", "Fluminense FC": "Fluminense FC",
    "Al Hilal SFC": "Al Hilal SFC", "Inter Miami CF": "Inter Miami CF",
    "FC Salzburg": "FC Salzburg", "Seattle Sounders FC": "Seattle Sounders FC",
    "Al Ahly FC": "Al Ahly FC", "CF Pachuca": "CF Pachuca",
    "Urawa Red Diamonds": "Urawa Red Diamonds", "Ulsan HD FC": "Ulsan HD FC",
    "Al Ain FC": "Al Ain FC", "CF Monterrey": "CF Monterrey",
    "Espérance Sportive de Tunis": "Espérance Sportive de Tunis",
    "Wydad AC": "Wydad AC", "Mamelodi Sundowns FC": "Mamelodi Sundowns FC",
    "Auckland City FC": "Auckland City FC", "LAFC": "LAFC",
}

# --- Team Details ---
TEAM_DETAILS = {
    "Al Ahly FC": {"team_id": "67b8be4865db8d4ef5b05df6", "short_code": "AHL", "api_id": 460, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Al%20Ahly%20FC%20round.png"},
    "Al Ain FC": {"team_id": "67b8be4c65db8d4ef5b05f00", "short_code": "AAN", "api_id": 7780, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Al%20Ain%20FC%20round.png"},
    "Al Hilal SFC": {"team_id": "67b8be4c65db8d4ef5b05ef8", "short_code": "HIL", "api_id": 7011, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Al%20Hilal%20round.png"},
    "Atlético de Madrid": {"team_id": "67b8be4c65db8d4ef5b05f03", "short_code": "ATM", "api_id": 7980, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Atl%C3%A9tico%20de%20Madrid%20round.png"},
    "Auckland City FC": {"team_id": "67b8be4965db8d4ef5b05e3d", "short_code": "AFC", "api_id": 1022, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Auckland%20City%20FC%20round.png"},
    "SL Benfica": {"team_id": "67b8be4865db8d4ef5b05e12", "short_code": "BEN", "api_id": 605, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/SL%20Benfica%20round.png"},
    "CA Boca Juniors": {"team_id": "67b8be4865db8d4ef5b05e0a", "short_code": "BOC", "api_id": 587, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/CA%20Boca%20Juniors%20round.png"},
    "Borussia Dortmund": {"team_id": "67b8be4665db8d4ef5b05db2", "short_code": "BVB", "api_id": 68, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Borussia%20Dortmund%20round.png"},
    "Botafogo FR": {"team_id": "67b8be4a65db8d4ef5b05e7f", "short_code": "BOT", "api_id": 2864, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Botafogo%20round.png"},
    "Chelsea FC": {"team_id": "67b8be4565db8d4ef5b05d90", "short_code": "CHE", "api_id": 18, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Chelsea%20FC%20round.png"},
    "Espérance Sportive de Tunis": {"team_id": "683e0419988d77e1048fd51c", "short_code": "EST", "api_id": 5832, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Esp%C3%A9rance%20Sportive%20de%20Tunis%20round.png"},
    "FC Bayern München": {"team_id": "67b8be4865db8d4ef5b05dfd", "short_code": "BAY", "api_id": 503, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/FC%20Bayern%20M%C3%BCnchen%20round.png"},
    "CR Flamengo": {"team_id": "67b8be4965db8d4ef5b05e3e", "short_code": "FLA", "api_id": 1024, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/CR%20Flamengo%20round.png"},
    "Fluminense FC": {"team_id": "67b8be4965db8d4ef5b05e43", "short_code": "FLU", "api_id": 1095, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Fluminense%20FC%20round.png"},
    "FC Internazionale Milano": {"team_id": "67b8be4a65db8d4ef5b05e82", "short_code": "INT", "api_id": 2930, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/FC%20Internazionale%20Milano%20round.png"},
    "Inter Miami CF": {"team_id": "67b8be4e65db8d4ef5b05f49", "short_code": "MIA", "api_id": 239235, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Inter%20Miami%20CF%20round.png"},
    "Juventus FC": {"team_id": "67b8be4865db8d4ef5b05e15", "short_code": "JUV", "api_id": 625, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Juventus%20FC%20round.png"},
    "LAFC": {"team_id": "683d905b988d77e1048fd503", "short_code": "LAF", "api_id": 147671, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/LAFC%20round.png"},
    "Mamelodi Sundowns FC": {"team_id": "67b8be4c65db8d4ef5b05ef1", "short_code": "MSF", "api_id": 6755, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Mamelodi%20Sundowns%20FC%20round.png"},
    "Manchester City FC": {"team_id": "67b8be4565db8d4ef5b05d87", "short_code": "MCI", "api_id": 9, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Manchester%20City%20FC%20round.png"},
    "CF Monterrey": {"team_id": "67b8be4a65db8d4ef5b05e6f", "short_code": "MON", "api_id": 2662, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/CF%20Monterrey%20round.png"},
    "CF Pachuca": {"team_id": "67b8be4d65db8d4ef5b05f17", "short_code": "PAC", "api_id": 10036, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/CF%20Pachuca%20round.png"},
    "SE Palmeiras": {"team_id": "67b8be4b65db8d4ef5b05e92", "short_code": "PAL", "api_id": 3422, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/SE%20Palmeiras%20round.png"},
    "Paris Saint-Germain": {"team_id": "67b8be4865db8d4ef5b05e0c", "short_code": "PSG", "api_id": 591, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Paris%20Saint-Germain%20round.png"},
    "FC Porto": {"team_id": "67b8be4865db8d4ef5b05e1b", "short_code": "POR", "api_id": 652, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/FC%20Porto%20round.png"},
    "Real Madrid CF": {"team_id": "67b8be4b65db8d4ef5b05e95", "short_code": "RMA", "api_id": 3468, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Real%20Madrid%20C.%20F.%20round.png"},
    "CA River Plate": {"team_id": "67b8be4d65db8d4ef5b05f16", "short_code": "RIV", "api_id": 10002, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/CA%20River%20Plate%20round.png"},
    "FC Salzburg": {"team_id": "67b8be4565db8d4ef5b05da6", "short_code": "SAL", "api_id": 49, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/FC%20Salzburg%20round.png"},
    "Seattle Sounders FC": {"team_id": "67b8be4a65db8d4ef5b05e6e", "short_code": "SEA", "api_id": 2649, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Seattle%20Sounders%20FC%20round.png"},
    "Ulsan HD FC": {"team_id": "67b8be4c65db8d4ef5b05ed6", "short_code": "UHD", "api_id": 5839, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Ulsan%20HD%20round.png"},
    "Urawa Red Diamonds": {"team_id": "67b8be4765db8d4ef5b05dd3", "short_code": "URD", "api_id": 280, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Urawa%20Red%20Diamonds%20round.png"},
    "Wydad AC": {"team_id": "67b8be4a65db8d4ef5b05e7e", "short_code": "WAC", "api_id": 2846, "image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Wydad%20AC%20round.png"}
}
DEFAULT_TEAM_DETAIL = {"team_id": "N/A_ID", "short_code": "N/A", "api_id": None, "image": "https://example.com/default_image.png"}

for _team_name_detail_key, _details_val in TEAM_DETAILS.items():
    if _team_name_detail_key not in TEAM_NAME_MAPPING:
        TEAM_NAME_MAPPING[_team_name_detail_key] = _team_name_detail_key
    if isinstance(_details_val.get("api_id"), int):
         TEAM_NAME_MAPPING[str(_details_val["api_id"])] = _team_name_detail_key

# --- Other Constants and Global Data Structures ---
DEFENSIVE_POSITIONS = ["Goalkeeper", "Centre-Back", "Left-Back", "Right-Back", "Sweeper", "Defender"]
OUTRIGHT_COMPONENT_WEIGHTS = {'base_strength_from_odds': 0.70, 'venue_impact': 0.20, 'fatigue': 0.10}
AVERAGE_TOTAL_GOALS_IN_MATCH = 2.7
MAX_POISSON_GOALS = 7

FULL_FIXTURE_DATA_RAW = """fixture_id	stage_name	starting_at	home_team_name	away_team_name	group_name	home_team_id	away_team_id	GW
67cfda1c36a76522457ee1b9	Group Stage	2025-06-15 0:00:00	Al Ahly	Inter Miami	Group A	67b8be4865db8d4ef5b05df6	67b8be4e65db8d4ef5b05f49	1
67cfda6736a76522457eeda4	Group Stage	2025-06-15 16:00:00	FC Bayern München	Auckland City	Group C	67b8be4865db8d4ef5b05dfd	67b8be4965db8d4ef5b05e3d	1
67cfda1b36a76522457ee1b8	Group Stage	2025-06-15 19:00:00	Paris Saint Germain	Atlético Madrid	Group B	67b8be4865db8d4ef5b05e0c	67b8be4c65db8d4ef5b05f03	1
67cfda1e36a76522457ee5a2	Group Stage	2025-06-15 22:00:00	Palmeiras	Porto	Group A	67b8be4b65db8d4ef5b05e92	67b8be4865db8d4ef5b05e1b	1
67cfda2436a76522457ee5a7	Group Stage	2025-06-16 2:00:00	Botafogo	Seattle Sounders	Group B	67b8be4a65db8d4ef5b05e7f	67b8be4a65db8d4ef5b05e6e	1
67cfda3836a76522457ee5b4	Group Stage	2025-06-16 19:00:00	Chelsea	Los Angeles FC	Group D	67b8be4565db8d4ef5b05d90	683d905b988d77e1048fd503	1
67cfda4c36a76522457ee9a9	Group Stage	2025-06-16 22:00:00	Boca Juniors	Benfica	Group C	67b8be4865db8d4ef5b05e0a	67b8be4865db8d4ef5b05e12	1
67cfda5236a76522457ee9af	Group Stage	2025-06-17 1:00:00	Flamengo	ES Tunis	Group D	67b8be4965db8d4ef5b05e3e	683e0419988d77e1048fd51c	1
67cfda4136a76522457ee9a2	Group Stage	2025-06-17 16:00:00	Fluminense	Borussia Dortmund	Group F	67b8be4965db8d4ef5b05e43	67b8be4665db8d4ef5b05db2	1
67cfda6236a76522457eeda1	Group Stage	2025-06-17 19:00:00	River Plate	Urawa Reds	Group E	67b8be4d65db8d4ef5b05f16	67b8be4765db8d4ef5b05dd3	1
67cfda4436a76522457ee9a4	Group Stage	2025-06-17 22:00:00	Ulsan HD	Mamelodi Sundowns	Group F	67b8be4c65db8d4ef5b05ed6	67b8be4c65db8d4ef5b05ef1	1
67cfda3c36a76522457ee5b7	Group Stage	2025-06-18 1:00:00	Monterrey	Inter	Group E	67b8be4a65db8d4ef5b05e6f	67b8be4a65db8d4ef5b05e82	1
67cfda3b36a76522457ee5b6	Group Stage	2025-06-18 16:00:00	Manchester City	Wydad Casablanca	Group G	67b8be4565db8d4ef5b05d87	67b8be4a65db8d4ef5b05e7e	1
67cfda7336a76522457eedac	Group Stage	2025-06-18 19:00:00	Real Madrid	Al Hilal	Group H	67b8be4b65db8d4ef5b05e95	67b8be4c65db8d4ef5b05ef8	1
67cfda3f36a76522457ee9a1	Group Stage	2025-06-18 22:00:00	Pachuca	Salzburg	Group H	67b8be4d65db8d4ef5b05f17	67b8be4565db8d4ef5b05da6	1
67cfda2936a76522457ee5aa	Group Stage	2025-06-19 1:00:00	Al Ain	Juventus	Group G	67b8be4c65db8d4ef5b05f00	67b8be4865db8d4ef5b05e15	1
67cfda2136a76522457ee5a4	Group Stage	2025-06-19 16:00:00	Palmeiras	Al Ahly	Group A	67b8be4b65db8d4ef5b05e92	67b8be4865db8d4ef5b05df6	2
67cfda2036a76522457ee5a3	Group Stage	2025-06-19 19:00:00	Inter Miami	Porto	Group A	67b8be4e65db8d4ef5b05f49	67b8be4865db8d4ef5b05e1b	2
67cfda1836a76522457ee1b6	Group Stage	2025-06-19 22:00:00	Seattle Sounders	Atlético Madrid	Group B	67b8be4a65db8d4ef5b05e6e	67b8be4c65db8d4ef5b05f03	2
67cfda2336a76522457ee5a6	Group Stage	2025-06-20 1:00:00	Paris Saint Germain	Botafogo	Group B	67b8be4865db8d4ef5b05e0c	67b8be4a65db8d4ef5b05e7f	2
67cfda4d36a76522457ee9aa	Group Stage	2025-06-20 16:00:00	Benfica	Auckland City	Group C	67b8be4865db8d4ef5b05e12	67b8be4965db8d4ef5b05e3d	2
67cfda6836a76522457eeda5	Group Stage	2025-06-20 18:00:00	Flamengo	Chelsea	Group D	67b8be4965db8d4ef5b05e3e	67b8be4565db8d4ef5b05d90	2
67cfda4f36a76522457ee9ac	Group Stage	2025-06-20 22:00:00	Los Angeles FC	ES Tunis	Group D	683d905b988d77e1048fd503	683e0419988d77e1048fd51c	2
67cfda4936a76522457ee9a7	Group Stage	2025-06-21 1:00:00	FC Bayern München	Boca Juniors	Group C	67b8be4865db8d4ef5b05dfd	67b8be4865db8d4ef5b05e0a	2
67cfda3e36a76522457ee9a0	Group Stage	2025-06-21 16:00:00	Mamelodi Sundowns	Borussia Dortmund	Group F	67b8be4c65db8d4ef5b05ef1	67b8be4665db8d4ef5b05db2	2
67cfda5436a76522457ee9b0	Group Stage	2025-06-21 19:00:00	Inter	Urawa Reds	Group E	67b8be4a65db8d4ef5b05e82	67b8be4765db8d4ef5b05dd3	2
67cfda3936a76522457ee5b5	Group Stage	2025-06-21 22:00:00	Fluminense	Ulsan HD	Group F	67b8be4965db8d4ef5b05e43	67b8be4c65db8d4ef5b05ed6	2
67cfda7136a76522457eedab	Group Stage	2025-06-22 1:00:00	River Plate	Monterrey	Group E	67b8be4d65db8d4ef5b05f16	67b8be4a65db8d4ef5b05e6f	2
67cfda6e36a76522457eeda9	Group Stage	2025-06-22 16:00:00	Juventus	Wydad Casablanca	Group G	67b8be4865db8d4ef5b05e15	67b8be4a65db8d4ef5b05e7e	2
67cfda6b36a76522457eeda7	Group Stage	2025-06-22 19:00:00	Real Madrid	Pachuca	Group H	67b8be4b65db8d4ef5b05e95	67b8be4d65db8d4ef5b05f17	2
67cfda4236a76522457ee9a3	Group Stage	2025-06-22 22:00:00	Salzburg	Al Hilal	Group H	67b8be4565db8d4ef5b05da6	67b8be4c65db8d4ef5b05ef8	2
67cfda6536a76522457eeda3	Group Stage	2025-06-23 1:00:00	Manchester City	Al Ain	Group G	67b8be4565db8d4ef5b05d87	67b8be4c65db8d4ef5b05f00	2
67cfda1636a76522457ee1b5	Group Stage	2025-06-23 19:00:00	Seattle Sounders	Paris Saint Germain	Group B	67b8be4a65db8d4ef5b05e6e	67b8be4865db8d4ef5b05e0c	3
67cfda1936a76522457ee1b7	Group Stage	2025-06-23 19:00:00	Atlético Madrid	Botafogo	Group B	67b8be4c65db8d4ef5b05f03	67b8be4a65db8d4ef5b05e7f	3
67cfda1336a76522457ee1b3	Group Stage	2025-06-24 1:00:00	Inter Miami	Palmeiras	Group A	67b8be4e65db8d4ef5b05f49	67b8be4b65db8d4ef5b05e92	3
67cfda1536a76522457ee1b4	Group Stage	2025-06-24 1:00:00	Porto	Al Ahly	Group A	67b8be4865db8d4ef5b05e1b	67b8be4865db8d4ef5b05df6	3
67cfda5536a76522457ee9b1	Group Stage	2025-06-24 19:00:00	Auckland City	Boca Juniors	Group C	67b8be4965db8d4ef5b05e3d	67b8be4865db8d4ef5b05e0a	3
67cfda5836a76522457ee9b3	Group Stage	2025-06-24 19:00:00	Benfica	FC Bayern München	Group C	67b8be4865db8d4ef5b05e12	67b8be4865db8d4ef5b05dfd	3
67cfda4a36a76522457ee9a8	Group Stage	2025-06-25 1:00:00	Los Angeles FC	Flamengo	Group D	683d905b988d77e1048fd503	67b8be4965db8d4ef5b05e3e	3
67cfda6136a76522457eeda0	Group Stage	2025-06-25 1:00:00	ES Tunis	Chelsea	Group D	683e0419988d77e1048fd51c	67b8be4565db8d4ef5b05d90	3
67cfda5136a76522457ee9ae	Group Stage	2025-06-25 19:00:00	Mamelodi Sundowns	Fluminense	Group F	67b8be4c65db8d4ef5b05ef1	67b8be4965db8d4ef5b05e43	3
67cfda4636a76522457ee9a5	Group Stage	2025-06-25 19:00:00	Borussia Dortmund	Ulsan HD	Group F	67b8be4665db8d4ef5b05db2	67b8be4c65db8d4ef5b05ed6	3
67cfda2736a76522457ee5a9	Group Stage	2025-06-26 1:00:00	Urawa Reds	Monterrey	Group E	67b8be4765db8d4ef5b05dd3	67b8be4a65db8d4ef5b05e6f	3
67cfda3636a76522457ee5b3	Group Stage	2025-06-26 1:00:00	Inter	River Plate	Group E	67b8be4a65db8d4ef5b05e82	67b8be4d65db8d4ef5b05f16	3
67cfda4736a76522457ee9a6	Group Stage	2025-06-26 19:00:00	Juventus	Manchester City	Group G	67b8be4865db8d4ef5b05e15	67b8be4565db8d4ef5b05d87	3
67cfda5736a76522457ee9b2	Group Stage	2025-06-26 19:00:00	Wydad Casablanca	Al Ain	Group G	67b8be4a65db8d4ef5b05e7e	67b8be4c65db8d4ef5b05f00	3
67cfda5d36a76522457eebcf	Group Stage	2025-06-27 1:00:00	Al Hilal	Pachuca	Group H	67b8be4c65db8d4ef5b05ef8	67b8be4d65db8d4ef5b05f17	3
67cfda6d36a76522457eeda8	Group Stage	2025-06-27 1:00:00	Salzburg	Real Madrid	Group H	67b8be4565db8d4ef5b05da6	67b8be4b65db8d4ef5b05e95	3"""

USER_PROVIDED_FIXTURES_WITH_STADIUMS_RAW = [
    {'home_team': 'Al Ahly FC', 'away_team': 'Inter Miami CF', 'date': '2025-06-15', 'time': '12:00 AM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'A'},
    {'home_team': 'SE Palmeiras', 'away_team': 'FC Porto', 'date': '2025-06-15', 'time': '10:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'A'},
    {'home_team': 'Paris Saint-Germain', 'away_team': 'Atlético de Madrid', 'date': '2025-06-15', 'time': '07:00 PM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'B'},
    {'home_team': 'Botafogo FR', 'away_team': 'Seattle Sounders FC', 'date': '2025-06-16', 'time': '02:00 AM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'B'},
    {'home_team': 'FC Bayern München', 'away_team': 'Auckland City FC', 'date': '2025-06-15', 'time': '04:00 PM', 'stadium': 'TQL Stadium, Cincinnati, OH', 'group': 'C'},
    {'home_team': 'CA Boca Juniors', 'away_team': 'SL Benfica', 'date': '2025-06-16', 'time': '10:00 PM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'C'},
    {'home_team': 'CR Flamengo', 'away_team': 'Espérance Sportive de Tunis', 'date': '2025-06-17', 'time': '01:00 AM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'D'},
    {'home_team': 'Chelsea FC', 'away_team': 'LAFC', 'date': '2025-06-16', 'time': '07:00 PM', 'stadium': 'Mercedes-Benz Stadium, Atlanta, GA', 'group': 'D'},
    {'home_team': 'CA River Plate', 'away_team': 'Urawa Red Diamonds', 'date': '2025-06-17', 'time': '07:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'E'},
    {'home_team': 'CF Monterrey', 'away_team': 'FC Internazionale Milano', 'date': '2025-06-18', 'time': '01:00 AM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'E'},
    {'home_team': 'Fluminense FC', 'away_team': 'Borussia Dortmund', 'date': '2025-06-17', 'time': '04:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'F'},
    {'home_team': 'Ulsan HD FC', 'away_team': 'Mamelodi Sundowns FC', 'date': '2025-06-17', 'time': '10:00 PM', 'stadium': 'Inter&Co Stadium, Orlando, FL', 'group': 'F'},
    {'home_team': 'Manchester City FC', 'away_team': 'Wydad AC', 'date': '2025-06-18', 'time': '04:00 PM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'G'},
    {'home_team': 'Al Ain FC', 'away_team': 'Juventus FC', 'date': '2025-06-19', 'time': '01:00 AM', 'stadium': 'Audi Field, Washington, D.C.', 'group': 'G'},
    {'home_team': 'Real Madrid CF', 'away_team': 'Al Hilal SFC', 'date': '2025-06-18', 'time': '07:00 PM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'H'},
    {'home_team': 'CF Pachuca', 'away_team': 'FC Salzburg', 'date': '2025-06-18', 'time': '10:00 PM', 'stadium': 'TQL Stadium, Cincinnati, OH', 'group': 'H'},
    {'home_team': 'SE Palmeiras', 'away_team': 'Al Ahly FC', 'date': '2025-06-19', 'time': '04:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'A'},
    {'home_team': 'Inter Miami CF', 'away_team': 'FC Porto', 'date': '2025-06-19', 'time': '07:00 PM', 'stadium': 'Mercedes-Benz Stadium, Atlanta, GA', 'group': 'A'},
    {'home_team': 'Paris Saint-Germain', 'away_team': 'Botafogo FR', 'date': '2025-06-20', 'time': '01:00 AM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'B'},
    {'home_team': 'Seattle Sounders FC', 'away_team': 'Atlético de Madrid', 'date': '2025-06-19', 'time': '10:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'B'},
    {'home_team': 'FC Bayern München', 'away_team': 'CA Boca Juniors', 'date': '2025-06-21', 'time': '01:00 AM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'C'},
    {'home_team': 'SL Benfica', 'away_team': 'Auckland City FC', 'date': '2025-06-20', 'time': '04:00 PM', 'stadium': 'Inter&Co Stadium, Orlando, FL', 'group': 'C'},
    {'home_team': 'CR Flamengo', 'away_team': 'Chelsea FC', 'date': '2025-06-20', 'time': '06:00 PM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'D'}, 
    {'home_team': 'LAFC', 'away_team': 'Espérance Sportive de Tunis', 'date': '2025-06-20', 'time': '10:00 PM', 'stadium': 'GEODIS Park, Nashville, TN', 'group': 'D'},
    {'home_team': 'CA River Plate', 'away_team': 'CF Monterrey', 'date': '2025-06-22', 'time': '01:00 AM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'E'},
    {'home_team': 'FC Internazionale Milano', 'away_team': 'Urawa Red Diamonds', 'date': '2025-06-21', 'time': '07:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'E'},
    {'home_team': 'Fluminense FC', 'away_team': 'Ulsan HD FC', 'date': '2025-06-21', 'time': '10:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'F'},
    {'home_team': 'Mamelodi Sundowns FC', 'away_team': 'Borussia Dortmund', 'date': '2025-06-21', 'time': '04:00 PM', 'stadium': 'TQL Stadium, Cincinnati, OH', 'group': 'F'},
    {'home_team': 'Manchester City FC', 'away_team': 'Al Ain FC', 'date': '2025-06-23', 'time': '01:00 AM', 'stadium': 'Mercedes-Benz Stadium, Atlanta, GA', 'group': 'G'},
    {'home_team': 'Juventus FC', 'away_team': 'Wydad AC', 'date': '2025-06-22', 'time': '04:00 PM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'G'},
    {'home_team': 'Real Madrid CF', 'away_team': 'CF Pachuca', 'date': '2025-06-22', 'time': '07:00 PM', 'stadium': 'Bank of America Stadium, Charlotte, NC', 'group': 'H'},
    {'home_team': 'FC Salzburg', 'away_team': 'Al Hilal SFC', 'date': '2025-06-22', 'time': '10:00 PM', 'stadium': 'Audi Field, Washington, D.C.', 'group': 'H'},
    {'home_team': 'FC Porto', 'away_team': 'Al Ahly FC', 'date': '2025-06-24', 'time': '01:00 AM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'A'},
    {'home_team': 'Inter Miami CF', 'away_team': 'SE Palmeiras', 'date': '2025-06-24', 'time': '01:00 AM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'A'},
    {'home_team': 'Atlético de Madrid', 'away_team': 'Botafogo FR', 'date': '2025-06-23', 'time': '07:00 PM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'B'},
    {'home_team': 'Seattle Sounders FC', 'away_team': 'Paris Saint-Germain', 'date': '2025-06-23', 'time': '07:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'B'},
    {'home_team': 'Auckland City FC', 'away_team': 'CA Boca Juniors', 'date': '2025-06-24', 'time': '07:00 PM', 'stadium': 'GEODIS Park, Nashville, TN', 'group': 'C'}, 
    {'home_team': 'SL Benfica', 'away_team': 'FC Bayern München', 'date': '2025-06-24', 'time': '07:00 PM', 'stadium': 'Bank of America Stadium, Charlotte, NC', 'group': 'C'},
    {'home_team': 'Espérance Sportive de Tunis', 'away_team': 'Chelsea FC', 'date': '2025-06-25', 'time': '01:00 AM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'D'},
    {'home_team': 'LAFC', 'away_team': 'CR Flamengo', 'date': '2025-06-25', 'time': '01:00 AM', 'stadium': 'Camping World Stadium, Orlando, FL', 'group': 'D'},
    {'home_team': 'Urawa Red Diamonds', 'away_team': 'CF Monterrey', 'date': '2025-06-26', 'time': '01:00 AM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'E'},
    {'home_team': 'FC Internazionale Milano', 'away_team': 'CA River Plate', 'date': '2025-06-26', 'time': '01:00 AM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'E'},
    {'home_team': 'Borussia Dortmund', 'away_team': 'Ulsan HD FC', 'date': '2025-06-25', 'time': '07:00 PM', 'stadium': 'TQL Stadium, Cincinnati, OH', 'group': 'F'}, 
    {'home_team': 'Mamelodi Sundowns FC', 'away_team': 'Fluminense FC', 'date': '2025-06-25', 'time': '07:00 PM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'F'}, 
    {'home_team': 'Wydad AC', 'away_team': 'Al Ain FC', 'date': '2025-06-26', 'time': '07:00 PM', 'stadium': 'Audi Field, Washington, D.C.', 'group': 'G'}, 
    {'home_team': 'Juventus FC', 'away_team': 'Manchester City FC', 'date': '2025-06-26', 'time': '07:00 PM', 'stadium': 'Camping World Stadium, Orlando, FL', 'group': 'G'}, 
    {'home_team': 'Al Hilal SFC', 'away_team': 'CF Pachuca', 'date': '2025-06-27', 'time': '01:00 AM', 'stadium': 'GEODIS Park, Nashville, TN', 'group': 'H'},
    {'home_team': 'FC Salzburg', 'away_team': 'Real Madrid CF', 'date': '2025-06-27', 'time': '01:00 AM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'H'}
]

# Global Data Structures
FIXTURE_LOOKUP_MAP: Dict[FrozenSet[str], Dict[str, Any]] = {} 
TEAM_CS_PERCENTAGES_CACHE: Dict[str, Dict[str, float]] = {} 
FIXTURE_ID_TO_CS_CACHE_KEY_MAP: Dict[str, str] = {}
FIXTURE_ID_GW_LOOKUP: Dict[Tuple[str, str, str], Dict[str, str]] = {}
ALL_BASE_FIXTURES: List[Dict[str, Any]] = []
PLAYER_STATS_DF: Optional[pd.DataFrame] = None
TEAM_SEASON_STATS: Dict[str, Dict[str, float]] = {}
CS_ODDS_LOOKUP: Dict[Tuple[str, str, str], Dict[str, float]] = {}
DIRECT_AGS_DATA_FROM_JSON: Optional[Dict[str, Any]] = None
TEAM_STRENGTH_METRICS: Dict[str, float] = {}
MATCH_HISTORY_CONTEXTS: List[Dict[str, Optional[Dict[str, Any]]]] = []
FIXTURE_FDR_METRICS_CACHE: Dict[str, Dict[str, float]] = {}

# --- Pydantic Models ---
class TeamCleanSheet(BaseModel):
    match_identifier: str; fixture_id: str; GW: str; team_id: str
    team_name_original: str; team_name_canonical: str; short_code: str
    api_id: Any; clean_sheet_percentage: float; image_url: str

class TopCorrectScoreItem(BaseModel):
    score: str; percentage: Any

class TopCorrectScores(BaseModel):
    match_identifier: str; fixture_id: str; GW: str
    top_scores: List[TopCorrectScoreItem]

class DefensivePlayerCleanSheetInfo(BaseModel):
    player_name: str; player_id: Optional[str] = None; player_api_id: Optional[Any] = None
    team_name_canonical: str; team_id: str; team_short_code: str; team_api_id: Any
    position: str; clean_sheet_percentage: float; team_image_url: str

class MatchWithPlayerCleanSheets(BaseModel):
    match_identifier: str; fixture_id: str; GW: str
    defensive_players: List[DefensivePlayerCleanSheetInfo]

class PlayerCombinedStats(BaseModel):
    player_name: str; player_id: Optional[str] = None; player_api_id: Optional[str] = None
    team_name_canonical: str; team_api_id: Optional[Any] = None; team_short_code: str
    Position: Optional[str] = None; player_display_name: Optional[str] = None
    player_price: Optional[Any] = None; player_image: Optional[str] = None
    anytime_goalscorer_probability: float; ags_prob_source: str
    anytime_assist_probability: float; aas_prob_source: str
    clean_sheet_probability: float

class MatchWithPlayerCombinedStats(BaseModel):
    fixture_id: str; GW: str; date_str: str
    home_team_canonical: str; away_team_canonical: str
    home_team_xg: Optional[float] = None; away_team_xg: Optional[float] = None
    xg_source: Optional[str] = None
    players: List[PlayerCombinedStats]

# --- Helper Functions ---
def load_json_data(file_path: str) -> Optional[Dict]:
    if not os.path.exists(file_path):
        print(f"ERROR: Data file '{file_path}' not found.")
        if file_path in [CORRECT_SCORE_FILE_PATH, UNIFIED_ANYTIME_GOALSCORER_FILE_PATH]:
             raise FileNotFoundError(f"Critical data file '{file_path}' not found.")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
        return data
    except FileNotFoundError: raise HTTPException(status_code=500, detail=f"Data file '{file_path}' not found.")
    except json.JSONDecodeError: raise HTTPException(status_code=500, detail=f"Error decoding JSON: '{file_path}'.")
    except Exception as e: raise HTTPException(status_code=500, detail=f"Error loading '{file_path}': {e}")

def get_canonical_team_name(name_from_source: Any, mapping: Dict[str, str]) -> str:
    name_from_source_stripped = str(name_from_source).strip()
    if not name_from_source_stripped: return "N/A_EmptyName"
    if name_from_source_stripped in mapping: return mapping[name_from_source_stripped]
    if name_from_source_stripped in mapping.values(): return name_from_source_stripped
    name_lower = name_from_source_stripped.lower()
    for map_key, canonical_val in mapping.items():
        if name_lower == map_key.lower(): return canonical_val
    suffixes_to_remove = [" fc", " cf", " rj", " fr", " hd", " sc", " ac", " c.f.", " c. f.", " c f", " de ", " e ", " ba", " münchen", " riyadh", " abu dhabi", " casablanca", " hyundai", " football club", " de futebol e regatas", "esportiva ", "athletic "]
    punctuation_to_remove = ".()-&"
    temp_name_norm = name_lower
    for suffix in suffixes_to_remove: temp_name_norm = temp_name_norm.replace(suffix, "")
    for punc in punctuation_to_remove: temp_name_norm = temp_name_norm.replace(punc, "")
    temp_name_norm = temp_name_norm.strip().replace(" ", "") 
    for map_key, canonical_val in mapping.items():
        map_key_norm = map_key.lower()
        for suffix in suffixes_to_remove: map_key_norm = map_key_norm.replace(suffix, "")
        for punc in punctuation_to_remove: map_key_norm = map_key_norm.replace(punc, "")
        map_key_norm = map_key_norm.strip().replace(" ", "")
        if temp_name_norm == map_key_norm: return canonical_val
        canonical_val_norm = canonical_val.lower()
        for suffix in suffixes_to_remove: canonical_val_norm = canonical_val_norm.replace(suffix, "")
        for punc in punctuation_to_remove: canonical_val_norm = canonical_val_norm.replace(punc, "")
        canonical_val_norm = canonical_val_norm.strip().replace(" ", "")
        if temp_name_norm == canonical_val_norm: return canonical_val
    if name_from_source_stripped.isdigit() and name_from_source_stripped in mapping:
        return mapping[name_from_source_stripped]
    return name_from_source_stripped

def load_and_prepare_fixture_data_for_app1_lookup(raw_data_string: str, team_mapping: Dict[str, str]):
    global FIXTURE_LOOKUP_MAP
    FIXTURE_LOOKUP_MAP = {} 
    data_io = io.StringIO(raw_data_string)
    reader = csv.reader(data_io, delimiter='\t')
    try: header = next(reader)
    except StopIteration: print("ERROR: Fixture data string for App1 lookup is empty."); return
    for i, row in enumerate(reader):
        if len(row) < 9: continue
        fixture_id_fixture = row[0].strip()
        home_team_original_fixture = row[3].strip()
        away_team_original_fixture = row[4].strip()
        gw_fixture = row[8].strip()
        fixture_datetime_str = row[2].strip()
        fixture_date_fixture = fixture_datetime_str.split(" ")[0] if fixture_datetime_str else "N/A_Date"
        group_fixture = row[5].strip()
        if not home_team_original_fixture or not away_team_original_fixture or \
           "Winner Match" in home_team_original_fixture or "1st Group" in home_team_original_fixture or \
           "Winner Match" in away_team_original_fixture or "2nd Group" in away_team_original_fixture:
            continue
        canonical_home_fixture = get_canonical_team_name(home_team_original_fixture, team_mapping)
        canonical_away_fixture = get_canonical_team_name(away_team_original_fixture, team_mapping)
        if canonical_home_fixture.startswith("N/A_") or canonical_away_fixture.startswith("N/A_"): continue
        map_key = frozenset({canonical_home_fixture, canonical_away_fixture})
        fixture_details_to_store = {
            "fixture_id": fixture_id_fixture, "GW": gw_fixture, "fixture_date": fixture_date_fixture,
            "group": group_fixture, "_fixture_original_home": home_team_original_fixture, 
            "_fixture_original_away": away_team_original_fixture, "_canonical_home_fixture": canonical_home_fixture,
            "_canonical_away_fixture": canonical_away_fixture,
        }
        FIXTURE_LOOKUP_MAP[map_key] = fixture_details_to_store
    print(f"INFO: App1 Fixture lookup map populated with {len(FIXTURE_LOOKUP_MAP)} entries.")

def _populate_fixture_id_gw_lookup_for_app2(raw_data_string: str, team_mapping: Dict[str, str]):
    global FIXTURE_ID_GW_LOOKUP
    FIXTURE_ID_GW_LOOKUP = {}
    data_io = io.StringIO(raw_data_string)
    reader = csv.reader(data_io, delimiter='\t')
    try: header = next(reader)
    except StopIteration: print("ERROR: Fixture ID/GW data string for App2 is empty."); return
    col_indices = {name.strip(): i for i, name in enumerate(header)}
    required_cols = ['fixture_id', 'home_team_name', 'away_team_name', 'starting_at', 'GW']
    if not all(col in col_indices for col in required_cols):
        print(f"ERROR: Missing required columns in fixture data for App2 ID/GW lookup. Expected: {required_cols}, Got: {header}"); return
    for i, row in enumerate(reader):
        if len(row) < len(header): continue
        try:
            fixture_id_fixture = row[col_indices['fixture_id']].strip()
            home_team_original = row[col_indices['home_team_name']].strip()
            away_team_original = row[col_indices['away_team_name']].strip()
            starting_at_str = row[col_indices['starting_at']].strip()
            gw_fixture = row[col_indices['GW']].strip()
            if not all([fixture_id_fixture, home_team_original, away_team_original, starting_at_str, gw_fixture]): continue
            fixture_date_str = starting_at_str.split(" ")[0]
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", fixture_date_str): continue
            canonical_home = get_canonical_team_name(home_team_original, team_mapping)
            canonical_away = get_canonical_team_name(away_team_original, team_mapping)
            if "N/A" in canonical_home or "N/A" in canonical_away or canonical_home.startswith("N/A_") or canonical_away.startswith("N/A_"): continue
            lookup_key = (canonical_home, canonical_away, fixture_date_str) 
            FIXTURE_ID_GW_LOOKUP[lookup_key] = {"fixture_id": fixture_id_fixture, "GW": gw_fixture}
        except Exception as e: print(f"ERROR processing App2 fixture row {i+2} for ID/GW lookup: {row} - {e}")
    print(f"INFO: App2 Fixture ID/GW lookup populated with {len(FIXTURE_ID_GW_LOOKUP)} entries.")

def create_base_fixtures_with_canonical_names_from_hardcoded_for_app2(
    user_provided_fixtures: List[Dict[str, Any]],
    team_map: Dict[str, str],
    fixture_id_gw_provider: Dict[Tuple[str, str, str], Dict[str, str]]
) -> List[Dict[str, Any]]:
    global ALL_BASE_FIXTURES
    ALL_BASE_FIXTURES = []
    processed_fixtures_temp = []
    for fix_data in user_provided_fixtures:
        home_raw, away_raw = str(fix_data.get('home_team','')).strip(), str(fix_data.get('away_team','')).strip()
        home_c, away_c = get_canonical_team_name(home_raw, team_map), get_canonical_team_name(away_raw, team_map)
        date_s, time_s = fix_data.get('date'), fix_data.get('time')
        if not all([home_c, away_c, date_s, time_s]) or home_c == away_c or home_c.startswith("N/A_") or away_c.startswith("N/A_"): continue
        
        try:
            time_s_corrected = time_s 
            if time_s == "24:00":
                dt_obj = datetime_cls.strptime(date_s, '%Y-%m-%d') + timedelta(days=1)
                time_s_corrected = "12:00 AM" 
            else: # Handles "12:00 AM", "01:00 PM", etc.
                dt_obj = datetime_cls.strptime(f"{date_s} {time_s}", '%Y-%m-%d %I:%M %p')
            
            date_obj_only = dt_obj.date()

            fixture_extra_info = fixture_id_gw_provider.get(
                (home_c, away_c, date_s), 
                fixture_id_gw_provider.get((away_c, home_c, date_s), 
                {"fixture_id": f"NO_ID_FOR_{home_c}_vs_{away_c}_{date_s}", "GW": "N/A_GW"})
            )
            processed_fixtures_temp.append({
                'home_team_canonical': home_c, 'away_team_canonical': away_c,
                'date_str': date_s, 'time_str': time_s_corrected, 'stadium': fix_data.get('stadium'),
                'group': fix_data.get('group'), 'date_dt': date_obj_only, 'datetime_obj': dt_obj,
                'fixture_id': fixture_extra_info['fixture_id'], 'GW': fixture_extra_info['GW']
            })
        except ValueError as e: print(f"ERROR parsing fixture date/time for {fix_data}: {e}")
    
    final_fixtures_list, final_unique_keys = [], set()
    for fix in sorted(processed_fixtures_temp, key=lambda x: x['datetime_obj']):
        key_primary, key_fallback = fix['fixture_id'], (fix['home_team_canonical'], fix['away_team_canonical'], fix['date_str'])
        if not key_primary.startswith("NO_ID_FOR_"):
            if key_primary not in final_unique_keys:
                final_fixtures_list.append(fix); final_unique_keys.add(key_primary); final_unique_keys.add(key_fallback)
        else:
             if key_fallback not in final_unique_keys:
                final_fixtures_list.append(fix); final_unique_keys.add(key_fallback)
    ALL_BASE_FIXTURES = final_fixtures_list
    print(f"INFO: Created {len(ALL_BASE_FIXTURES)} unique base fixtures for App2.")
    return ALL_BASE_FIXTURES

def parse_html_for_odds(file_path):
    teams_data = []
    if not os.path.exists(file_path): return teams_data
    try:
        with open(file_path, 'r', encoding='utf-8') as f: html_content = f.read()
        soup = BeautifulSoup(html_content, 'html.parser') # Changed to html.parser
        for row in soup.select('div[data-testid="outrights-table-row"]'):
            team_name_el = row.select_one('div[data-testid="outrights-participant-name"] p')
            odds_el = row.select_one('div[data-testid="add-to-coupon-button"] p')
            if team_name_el and odds_el:
                team_name, odds_text = team_name_el.get_text(strip=True), odds_el.get_text(strip=True)
                try:
                    odds_val = float(odds_text[1:]) / 100 + 1 if odds_text.startswith('+') \
                        else 100 / float(odds_text[1:]) + 1 if odds_text.startswith('-') \
                        else float(odds_text)
                    teams_data.append({'raw_team_name': team_name, 'decimal_odds': odds_val})
                except ValueError: pass
    except Exception as e: print(f"ERROR parsing HTML outright odds {file_path}: {e}")
    return teams_data

def parse_markdown_for_odds(file_path):
    teams_data = []
    if not os.path.exists(file_path): return teams_data
    try:
        with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
        pattern = re.compile(r"!\[(?:.*?)\]\(https?://.*?\)\s*\n*\s*(.*?)\s*\n*\s*(?:\d+\.?\d*)\s*\n*\s*([+-]\d+)\s*", re.MULTILINE)
        matches = pattern.findall(content)
        for raw_name, odds_text in matches:
            try:
                raw_name, odds_text = raw_name.strip(), odds_text.strip()
                odds_val = float(odds_text[1:]) / 100 + 1.0 if odds_text.startswith('+') \
                    else 100.0 / float(odds_text[1:]) + 1.0 if odds_text.startswith('-') \
                    else None
                if odds_val: teams_data.append({'raw_team_name': raw_name, 'decimal_odds': odds_val})
            except ValueError: pass
    except Exception as e: print(f"ERROR parsing Markdown outright odds {file_path}: {e}")
    return teams_data

def get_tournament_outright_odds_data_for_app2(html_fp, md_fp, team_map):
    raw_data = parse_html_for_odds(html_fp)
    if not raw_data: raw_data = parse_markdown_for_odds(md_fp)
    if not raw_data: print("WARNING: No outright odds data loaded for App2."); return pd.DataFrame()
    processed_odds, seen_canonical_names = [], set()
    for item in raw_data:
        canonical_name = get_canonical_team_name(item['raw_team_name'], team_map)
        if canonical_name in seen_canonical_names or item['decimal_odds'] <= 1.0 or canonical_name.startswith("N/A_"): continue
        seen_canonical_names.add(canonical_name)
        processed_odds.append({'team_name_canonical': canonical_name, 'implied_prob': 1.0 / item['decimal_odds']})
    return pd.DataFrame(processed_odds) if processed_odds else pd.DataFrame()

def normalize_tournament_implied_probs_for_app2(df_odds, all_fixture_teams_canonical):
    global TEAM_STRENGTH_METRICS
    TEAM_STRENGTH_METRICS = {}
    default_strength_value = 10.0
    if df_odds.empty or 'implied_prob' not in df_odds.columns:
        for team_c in all_fixture_teams_canonical: TEAM_STRENGTH_METRICS[team_c] = default_strength_value
        print("WARNING: Outright odds DF empty. Using default strength for App2."); return TEAM_STRENGTH_METRICS
    df_valid = df_odds[df_odds['implied_prob'] > 0].copy()
    if df_valid.empty:
        for team_c in all_fixture_teams_canonical: TEAM_STRENGTH_METRICS[team_c] = default_strength_value
        print("WARNING: No valid implied probabilities. Using default strength for App2."); return TEAM_STRENGTH_METRICS
    total_implied_prob = df_valid['implied_prob'].sum()
    df_valid['norm_prob'] = 0.0 if total_implied_prob < 1e-9 else df_valid['implied_prob'] / total_implied_prob
    max_norm_prob = df_valid['norm_prob'].max()
    df_valid['strength_metric'] = default_strength_value if max_norm_prob < 1e-9 else (df_valid['norm_prob'] / max_norm_prob) * 90.0 + 10.0
    for _, row in df_valid.iterrows(): TEAM_STRENGTH_METRICS[row['team_name_canonical']] = row['strength_metric']
    for team_c in all_fixture_teams_canonical:
        if team_c not in TEAM_STRENGTH_METRICS: TEAM_STRENGTH_METRICS[team_c] = default_strength_value
    print(f"INFO: App2 Team strength metrics calculated for {len(TEAM_STRENGTH_METRICS)} teams.")
    return TEAM_STRENGTH_METRICS

def create_last_match_dates_history_for_app2(sorted_fixtures_canonical: List[Dict[str,Any]]):
    global MATCH_HISTORY_CONTEXTS
    MATCH_HISTORY_CONTEXTS = []
    team_last_match_info: Dict[str, Dict[str, Any]] = {
        team_c: None for fix in sorted_fixtures_canonical 
        for team_c in (fix['home_team_canonical'], fix['away_team_canonical'])
    }
    for fixture in sorted_fixtures_canonical:
        home_c, away_c = fixture['home_team_canonical'], fixture['away_team_canonical']
        current_match_context = {home_c: team_last_match_info[home_c].copy() if team_last_match_info[home_c] else None,
                                 away_c: team_last_match_info[away_c].copy() if team_last_match_info[away_c] else None}
        MATCH_HISTORY_CONTEXTS.append(current_match_context)
        match_date, match_stadium = fixture.get('date_dt'), fixture.get('stadium')
        if match_date and match_stadium: # match_date is already a date_cls object here
            team_last_match_info[home_c] = {'date': match_date, 'venue': match_stadium}
            team_last_match_info[away_c] = {'date': match_date, 'venue': match_stadium}
    print(f"INFO: App2 Match history contexts created for {len(MATCH_HISTORY_CONTEXTS)} fixtures.")

def get_venue_impact_for_app2(home_team_canonical: str, away_team_canonical: str, stadium: Optional[str]):
    HOME_VENUES_CWC = {"Hard Rock Stadium, Miami Gardens, FL": "Inter Miami CF", "Lumen Field, Seattle, WA": "Seattle Sounders FC"}
    venue_home_team_canonical = None
    if stadium:
        stadium_strip_lower = stadium.strip().lower()
        for venue_key, venue_home_team in HOME_VENUES_CWC.items():
            if stadium_strip_lower == venue_key.strip().lower():
                venue_home_team_canonical = get_canonical_team_name(venue_home_team, TEAM_NAME_MAPPING); break
    home_adv, away_disadv = -12, 8
    if venue_home_team_canonical == home_team_canonical: return home_adv, away_disadv
    if venue_home_team_canonical == away_team_canonical: return away_disadv, home_adv
    return 0, 0

def calculate_fatigue_impact_for_app2(team_canonical: str, current_match_date_obj: date_cls, last_match_info: Optional[Dict[str, Any]], cross_country_travel: bool = False):
    if not last_match_info or not last_match_info.get('date'): return -10
    
    last_match_date_val = last_match_info['date'] # This should be a date_cls object from create_last_match_dates_history_for_app2
    
    # Ensure current_match_date_obj is also a date_cls object (it should be from fixture['date_dt'])
    if not isinstance(current_match_date_obj, date_cls) or not isinstance(last_match_date_val, date_cls):
        # print(f"Warning: Invalid date types for fatigue. Current: {type(current_match_date_obj)}, Last: {type(last_match_date_val)}")
        return 0 
        
    rest_days = (current_match_date_obj - last_match_date_val).days
    if rest_days < 0 : return 15 
    fatigue_score = 15 if rest_days < 2 else 8 if rest_days == 2 else 0 if rest_days < 5 else -5 if rest_days < 7 else -10
    return fatigue_score + (5 if cross_country_travel else 0)

def calculate_outright_fdr_components_for_app2(fixture: Dict[str, Any], team_strengths_map: Dict[str, float], match_history_for_fixture: Optional[Dict[str, Any]]):
    global FIXTURE_FDR_METRICS_CACHE
    fixture_id = fixture.get('fixture_id', 'unknown_fixture')
    if fixture_id in FIXTURE_FDR_METRICS_CACHE: return FIXTURE_FDR_METRICS_CACHE[fixture_id]
    
    home_c, away_c = fixture['home_team_canonical'], fixture['away_team_canonical']
    date_dt_obj = fixture.get('date_dt') # This is a date_cls object
    stadium = fixture.get('stadium')

    if not date_dt_obj or not isinstance(date_dt_obj, date_cls): # Check type explicitly
        FIXTURE_FDR_METRICS_CACHE[fixture_id] = {'home_fdr_outright': 50.0, 'away_fdr_outright': 50.0}; return FIXTURE_FDR_METRICS_CACHE[fixture_id]
    
    h_base_fdr_component, a_base_fdr_component = team_strengths_map.get(away_c, 10.0), team_strengths_map.get(home_c, 10.0)
    ven_h_impact, ven_a_impact = get_venue_impact_for_app2(home_c, away_c, stadium)
    
    east_stadiums_lower = {s.lower().strip() for s in ["Hard Rock Stadium, Miami Gardens, FL", "MetLife Stadium, East Rutherford, NJ", "Lincoln Financial Field, Philadelphia, PA", "GEODIS Park, Nashville, TN", "Bank of America Stadium, Charlotte, NC", "Mercedes-Benz Stadium, Atlanta, GA", "Inter&Co Stadium, Orlando, FL", "Audi Field, Washington, D.C.", "Camping World Stadium, Orlando, FL", "TQL Stadium, Cincinnati, OH"]}
    west_stadiums_lower = {s.lower().strip() for s in ["Lumen Field, Seattle, WA", "Rose Bowl Stadium, Pasadena, CA"]}
    current_stadium_lower = stadium.lower().strip() if stadium else ""
    
    last_match_home_info = match_history_for_fixture.get(home_c) if match_history_for_fixture else None
    last_match_away_info = match_history_for_fixture.get(away_c) if match_history_for_fixture else None
    
    home_travel = last_match_home_info and last_match_home_info.get('venue') and current_stadium_lower and \
                  ((current_stadium_lower in east_stadiums_lower and last_match_home_info['venue'].lower().strip() in west_stadiums_lower) or \
                   (current_stadium_lower in west_stadiums_lower and last_match_home_info['venue'].lower().strip() in east_stadiums_lower))
    away_travel = last_match_away_info and last_match_away_info.get('venue') and current_stadium_lower and \
                  ((current_stadium_lower in east_stadiums_lower and last_match_away_info['venue'].lower().strip() in west_stadiums_lower) or \
                   (current_stadium_lower in west_stadiums_lower and last_match_away_info['venue'].lower().strip() in east_stadiums_lower))

    fat_h_impact = calculate_fatigue_impact_for_app2(home_c, date_dt_obj, last_match_home_info, home_travel)
    fat_a_impact = calculate_fatigue_impact_for_app2(away_c, date_dt_obj, last_match_away_info, away_travel)
    
    home_fdr_raw = (OUTRIGHT_COMPONENT_WEIGHTS['base_strength_from_odds'] * h_base_fdr_component + OUTRIGHT_COMPONENT_WEIGHTS['venue_impact'] * ven_h_impact + OUTRIGHT_COMPONENT_WEIGHTS['fatigue'] * fat_h_impact)
    away_fdr_raw = (OUTRIGHT_COMPONENT_WEIGHTS['base_strength_from_odds'] * a_base_fdr_component + OUTRIGHT_COMPONENT_WEIGHTS['venue_impact'] * ven_a_impact + OUTRIGHT_COMPONENT_WEIGHTS['fatigue'] * fat_a_impact)
    
    result = {'home_fdr_outright': round(np.clip(home_fdr_raw / 1.5 + 25, 1, 99),1), 'away_fdr_outright': round(np.clip(away_fdr_raw / 1.5 + 25, 1, 99),1)}
    FIXTURE_FDR_METRICS_CACHE[fixture_id] = result; return result

def estimate_xg_from_fdr_outrights_for_app2(h_fdr: Optional[float], a_fdr: Optional[float], avg_goals: float = AVERAGE_TOTAL_GOALS_IN_MATCH) -> Tuple[float, float]:
    if pd.isna(h_fdr) or pd.isna(a_fdr) or h_fdr is None or a_fdr is None: return avg_goals / 2.0, avg_goals / 2.0
    home_strength_proxy, away_strength_proxy = 1.0 / (h_fdr + 0.1), 1.0 / (a_fdr + 0.1)
    total_proxy_strength = home_strength_proxy + away_strength_proxy
    if total_proxy_strength < 1e-9: return avg_goals / 2.0, avg_goals / 2.0
    home_xg_ratio = home_strength_proxy / total_proxy_strength
    return max(0.1, home_xg_ratio * avg_goals), max(0.1, (1.0 - home_xg_ratio) * avg_goals)

def parse_cs_match_string_for_canonical_teams_for_app2(match_str: str, team_map: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    if not match_str or not isinstance(match_str, str): return None, None
    parts = None
    for sep in [' vs ', ' - ', ' @ ']:
        if sep in match_str: parts = match_str.split(sep, 1); break
    if not parts:
        parts_re = re.split(r'\s{2,}', match_str, 1)
        if len(parts_re) < 2:
            mid = len(match_str) // 2; space_after_mid, space_before_mid = match_str.find(' ', mid), match_str.rfind(' ', 0, mid)
            split_idx = space_after_mid if space_after_mid != -1 and (space_before_mid == -1 or abs(mid - space_after_mid) < abs(mid - space_before_mid)) else space_before_mid if space_before_mid != -1 else -1
            if split_idx != -1: parts = [match_str[:split_idx].strip(), match_str[split_idx+1:].strip()]
            elif len(match_str) > 10: parts = [match_str[:mid].strip(), match_str[mid:].strip()]
            else: return None, None
        else: parts = parts_re
    if not parts or len(parts) < 2: return None, None
    home_raw, away_raw = parts[0].strip(), parts[1].strip()
    home_canonical, away_canonical = get_canonical_team_name(home_raw, team_map), get_canonical_team_name(away_raw, team_map)
    if home_canonical.startswith("N/A_") or away_canonical.startswith("N/A_") or "UnknownTeam" in [home_raw, away_raw]: return None, None
    return home_canonical, away_canonical

def calculate_xg_from_cs_odds_for_app2(cs_odds_dict: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    if not cs_odds_dict: return None, None
    home_xg_calc, away_xg_calc, total_inv_odds_sum, score_goal_probs = 0.0, 0.0, 0.0, []
    for score_str, odd_value_any in cs_odds_dict.items():
        try:
            odd_value = float(odd_value_any)
            if odd_value <= 1.0: continue
            inv_odd = 1.0 / odd_value
            score_parts = str(score_str).split('-')
            if len(score_parts) == 2:
                score_goal_probs.append({'h_goals': int(score_parts[0]), 'a_goals': int(score_parts[1]), 'inv_odd': inv_odd})
                total_inv_odds_sum += inv_odd
        except (ValueError, TypeError): continue
    if total_inv_odds_sum == 0: return None, None
    for item in score_goal_probs:
        norm_prob = item['inv_odd'] / total_inv_odds_sum
        home_xg_calc += item['h_goals'] * norm_prob
        away_xg_calc += item['a_goals'] * norm_prob
    return round(home_xg_calc, 3), round(away_xg_calc, 3)

def get_player_direct_ags_prob_for_app2(player_name_to_match: str, excel_player_id: Optional[str], excel_player_api_id: Optional[str], player_team_canonical: str, match_home_canonical: str, match_away_canonical: str, match_date_str: str, direct_ags_json_data: Optional[Dict[str, Any]]) -> Optional[float]:
    if not direct_ags_json_data or 'matches' not in direct_ags_json_data: return None
    for match_entry in direct_ags_json_data['matches']:
        entry_home_c = get_canonical_team_name(match_entry.get('home_team', ''), TEAM_NAME_MAPPING)
        entry_away_c = get_canonical_team_name(match_entry.get('away_team', ''), TEAM_NAME_MAPPING)
        if frozenset({entry_home_c, entry_away_c}) == frozenset({match_home_canonical, match_away_canonical}) and match_entry.get('date') == match_date_str:
            for ags_player_data in match_entry.get('players', []):
                ags_player_name = str(ags_player_data.get('player', '')).strip()
                ags_player_team_c = get_canonical_team_name(str(ags_player_data.get('team', '')).strip(), TEAM_NAME_MAPPING)
                ags_player_id_json = str(ags_player_data.get('player_id')) if pd.notna(ags_player_data.get('player_id')) else None
                ags_player_api_id_json = str(ags_player_data.get('player_api_id')) if pd.notna(ags_player_data.get('player_api_id')) else None
                match_found = (excel_player_id and ags_player_id_json and excel_player_id == ags_player_id_json and ags_player_team_c == player_team_canonical) or \
                              (excel_player_api_id and ags_player_api_id_json and excel_player_api_id == ags_player_api_id_json and ags_player_team_c == player_team_canonical) or \
                              (ags_player_name.lower() == player_name_to_match.lower() and ags_player_team_c == player_team_canonical)
                if match_found:
                    try: 
                        odds = float(ags_player_data.get('odds')); return 1.0 / odds if odds > 1.0 else None
                    except (ValueError, TypeError): continue
    return None

# --- Calculation Logic Functions ---
def calculate_team_cs_percentages_logic(correct_score_data: Dict[str, Any], team_mapping: Dict[str, str], team_details_map: Dict[str, Dict[str, Any]], fixture_lookup: Dict[FrozenSet[str], Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not correct_score_data or 'matches' not in correct_score_data: return []
    team_clean_sheet_rows = []
    for match_info in correct_score_data['matches']:
        match_str, odds_dict, date, stadium = match_info.get('match'), match_info.get('correct_score_odds'), match_info.get('date', 'N/A_Date'), match_info.get('stadium', 'N/A_Stadium')
        if not match_str or not odds_dict: continue
        try:
            team_names = match_str.split(" vs ")
            if len(team_names) != 2: continue
            home_orig, away_orig = team_names[0].strip(), team_names[1].strip()
            home_canon, away_canon = get_canonical_team_name(home_orig, team_mapping), get_canonical_team_name(away_orig, team_mapping)
            if home_canon.startswith("N/A_") or away_canon.startswith("N/A_"): continue
        except Exception: continue
        fixture_data = fixture_lookup.get(frozenset({home_canon, away_canon}), {})
        fixture_id, gw = fixture_data.get("fixture_id", "N/A_FID"), fixture_data.get("GW", "N/A_GW")
        match_identifier = f"{match_str} ({date} at {stadium})"
        sum_probs_all, sum_probs_home_cs, sum_probs_away_cs = 0.0, 0.0, 0.0
        if not isinstance(odds_dict, dict): continue
        for score, odd_val_any in odds_dict.items():
            try:
                odd_val = float(odd_val_any)
                implied_prob = 1.0 / odd_val if odd_val > 0 else 0.0
                sum_probs_all += implied_prob
                s_parts = str(score).split('-'); 
                if len(s_parts) != 2: continue
                h_goals, a_goals = int(s_parts[0]), int(s_parts[1])
                if a_goals == 0: sum_probs_home_cs += implied_prob
                if h_goals == 0: sum_probs_away_cs += implied_prob
            except (ValueError, ZeroDivisionError, TypeError): continue
        home_cs_perc = (sum_probs_home_cs / sum_probs_all * 100.0) if sum_probs_all > 0 else 0.0
        away_cs_perc = (sum_probs_away_cs / sum_probs_all * 100.0) if sum_probs_all > 0 else 0.0
        home_details, away_details = team_details_map.get(home_canon, DEFAULT_TEAM_DETAIL), team_details_map.get(away_canon, DEFAULT_TEAM_DETAIL)
        team_clean_sheet_rows.append({'match_identifier': match_identifier, 'fixture_id': fixture_id, 'GW': gw, 'team_id': home_details["team_id"], 'team_name_original': home_orig, 'team_name_canonical': home_canon, 'short_code': home_details["short_code"], 'api_id': home_details["api_id"], 'clean_sheet_percentage': round(home_cs_perc, 2), 'image_url': home_details["image"]})
        team_clean_sheet_rows.append({'match_identifier': match_identifier, 'fixture_id': fixture_id, 'GW': gw, 'team_id': away_details["team_id"], 'team_name_original': away_orig, 'team_name_canonical': away_canon, 'short_code': away_details["short_code"], 'api_id': away_details["api_id"], 'clean_sheet_percentage': round(away_cs_perc, 2), 'image_url': away_details["image"]})
    return team_clean_sheet_rows

def calculate_top_scores_logic(correct_score_data: Dict[str, Any], team_mapping: Dict[str, str], fixture_lookup: Dict[FrozenSet[str], Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not correct_score_data or 'matches' not in correct_score_data: return []
    top_scores_output = []
    for match_info in correct_score_data['matches']:
        match_str, odds_dict, date, stadium = match_info.get('match'), match_info.get('correct_score_odds'), match_info.get('date', 'N/A_Date'), match_info.get('stadium', 'N/A_Stadium')
        if not match_str or not odds_dict or not isinstance(odds_dict, dict) or not odds_dict: continue
        try:
            team_names = match_str.split(" vs ")
            if len(team_names) != 2: continue
            home_orig, away_orig = team_names[0].strip(), team_names[1].strip()
            home_canon, away_canon = get_canonical_team_name(home_orig, team_mapping), get_canonical_team_name(away_orig, team_mapping)
            if home_canon.startswith("N/A_") or away_canon.startswith("N/A_"): continue
        except Exception: continue
        fixture_data = fixture_lookup.get(frozenset({home_canon, away_canon}), {})
        fixture_id, gw = fixture_data.get("fixture_id", "N/A_FID"), fixture_data.get("GW", "N/A_GW")
        match_identifier = f"{match_str} ({date} at {stadium})"
        score_probs, sum_implied_probs_all = [], 0.0
        for score, odd_val_any in odds_dict.items():
            try:
                odd_val = float(odd_val_any)
                implied_prob = 1.0 / odd_val if odd_val > 0 else 0.0
                score_probs.append({'score': score, 'implied_prob': implied_prob})
                sum_implied_probs_all += implied_prob
            except (ValueError, ZeroDivisionError, TypeError): continue
        if sum_implied_probs_all == 0:
            top_scores_output.append({'match_identifier': match_identifier, 'fixture_id': fixture_id, 'GW': gw, 'top_scores': [{'score': 'N/A', 'percentage': 'N/A'}]}); continue
        norm_scores = [{'score': item['score'], 'percentage': (item['implied_prob'] / sum_implied_probs_all) * 100.0} for item in score_probs]
        sorted_scores = sorted(norm_scores, key=lambda x: x['percentage'], reverse=True)
        top_scores_output.append({'match_identifier': match_identifier, 'fixture_id': fixture_id, 'GW': gw, 'top_scores': [{'score': s['score'], 'percentage': round(s['percentage'], 2)} for s in sorted_scores[:4]]})
    return top_scores_output

def calculate_player_clean_sheets_logic(anytime_goalscorer_data_app1: Dict[str, Any], team_cs_cache: Dict[str, Dict[str, float]], team_mapping: Dict[str, str], team_details_map: Dict[str, Dict[str, Any]], fixture_lookup: Dict[FrozenSet[str], Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not anytime_goalscorer_data_app1 or 'matches' not in anytime_goalscorer_data_app1: return []
    matches_with_players_dict: Dict[str, Dict[str, Any]] = {}
    for ag_match in anytime_goalscorer_data_app1['matches']:
        ag_home_orig, ag_away_orig, ag_date, ag_stadium, ag_players = ag_match.get("home_team"), ag_match.get("away_team"), ag_match.get("date"), ag_match.get("stadium", "N/A_Stadium"), ag_match.get("players", [])
        if not ag_home_orig or not ag_away_orig or not ag_date: continue
        ag_home_canon, ag_away_canon = get_canonical_team_name(ag_home_orig, team_mapping), get_canonical_team_name(ag_away_orig, team_mapping)
        if ag_home_canon.startswith("N/A_") or ag_away_canon.startswith("N/A_"): continue
        
        target_match_identifier_in_cache = None; home_cs_perc, away_cs_perc = 0.0, 0.0
        potential_keys = [f"{ag_home_orig} vs {ag_away_orig} ({ag_date} at {ag_stadium})", f"{ag_away_orig} vs {ag_home_orig} ({ag_date} at {ag_stadium})"]
        
        current_fixture_id, current_gw = "N/A_FID", "N/A_GW"
        fixture_data_ag = fixture_lookup.get(frozenset({ag_home_canon, ag_away_canon}))
        if fixture_data_ag: current_fixture_id, current_gw = fixture_data_ag.get("fixture_id", "N/A_FID"), fixture_data_ag.get("GW", "N/A_GW")

        found_cs = False
        for pk in potential_keys:
            if pk in team_cs_cache:
                target_match_identifier_in_cache = pk
                cs_data = team_cs_cache[pk]
                home_cs_perc, away_cs_perc = cs_data.get(ag_home_canon, 0.0), cs_data.get(ag_away_canon, 0.0)
                found_cs = True; break
        if not found_cs: 
            for cs_key, cs_match_data in team_cs_cache.items():
                try:
                    parts = cs_key.split(" ("); cs_teams_part = parts[0]; cs_date_stadium_part = parts[1][:-1]; cs_date_from_key = cs_date_stadium_part.split(" at ")[0]
                    cs_teams_split = cs_teams_part.split(" vs ")
                    if len(cs_teams_split) < 2: continue
                    cs_home_k_orig, cs_away_k_orig = cs_teams_split[0].strip(), cs_teams_split[1].strip()
                    cs_home_k_canon, cs_away_k_canon = get_canonical_team_name(cs_home_k_orig, team_mapping), get_canonical_team_name(cs_away_k_orig, team_mapping)
                    if frozenset({cs_home_k_canon, cs_away_k_canon}) == frozenset({ag_home_canon, ag_away_canon}) and cs_date_from_key == ag_date:
                        target_match_identifier_in_cache = cs_key
                        home_cs_perc, away_cs_perc = cs_match_data.get(ag_home_canon, 0.0), cs_match_data.get(ag_away_canon, 0.0)
                        found_cs = True; break
                except Exception: continue
        
        if not target_match_identifier_in_cache: target_match_identifier_in_cache = potential_keys[0]
        
        if target_match_identifier_in_cache not in matches_with_players_dict:
            matches_with_players_dict[target_match_identifier_in_cache] = {"match_identifier": target_match_identifier_in_cache, "fixture_id": current_fixture_id, "GW": current_gw, "defensive_players": []}

        for p_data in ag_players:
            p_name, p_team_orig, p_pos = p_data.get("player"), p_data.get("team"), p_data.get("position")
            if not p_name or not p_team_orig or not p_pos or p_pos not in DEFENSIVE_POSITIONS: continue
            p_team_canon = get_canonical_team_name(p_team_orig, team_mapping)
            if p_team_canon.startswith("N/A_"): continue
            p_cs_perc = home_cs_perc if p_team_canon == ag_home_canon else away_cs_perc if p_team_canon == ag_away_canon else 0.0
            p_team_details = team_details_map.get(p_team_canon, DEFAULT_TEAM_DETAIL)
            player_info = DefensivePlayerCleanSheetInfo(player_name=p_name, player_id=p_data.get("player_id"), player_api_id=p_data.get("player_api_id"), team_name_canonical=p_team_canon, team_id=p_team_details["team_id"], team_short_code=p_team_details["short_code"], team_api_id=p_team_details["api_id"], position=p_pos, clean_sheet_percentage=round(p_cs_perc, 2), team_image_url=p_team_details["image"])
            matches_with_players_dict[target_match_identifier_in_cache]["defensive_players"].append(player_info)
    return list(matches_with_players_dict.values())

def _calculate_player_specific_probabilities_for_app2(player_name: str, p_id_excel: Optional[str], p_api_id_excel: Optional[str], team_canon: str, p_goals: float, p_assists: float, team_goals: float, team_assists: float, team_match_xg: float, match_home_c: str, match_away_c: str, match_date_s: str, direct_ags_src: Optional[Dict[str,Any]], xg_src_str: str) -> Tuple[float, str, float, str]:
    ags_prob_0_1, aas_prob_0_1 = 0.0, 0.0; ags_src, aas_src = "not_set", "not_set"
    direct_ags_prob = get_player_direct_ags_prob_for_app2(player_name, p_id_excel, p_api_id_excel, team_canon, match_home_c, match_away_c, match_date_s, direct_ags_src)
    if direct_ags_prob is not None:
        ags_prob_0_1, ags_src = direct_ags_prob, "direct_odds"
    else:
        if team_goals > 0 and p_goals >= 0 and team_match_xg > 0:
            p_ind_xg = (p_goals / team_goals) * team_match_xg
            if p_ind_xg > 0: ags_prob_0_1 = 1.0 - poisson.pmf(0, p_ind_xg)
        ags_src = f"estimated_poisson_from_{xg_src_str}" + ("_low_prob" if p_goals > 0 and ags_prob_0_1 == 0 else "_no_season_goals_or_low_xg" if ags_prob_0_1 == 0 else "")
    if team_assists > 0 and p_assists >= 0 and team_match_xg > 0:
        p_ind_xa = (p_assists / team_assists) * team_match_xg
        if p_ind_xa > 0: aas_prob_0_1 = 1.0 - poisson.pmf(0, p_ind_xa)
    aas_src = f"estimated_poisson_from_{xg_src_str}" + ("_low_prob" if p_assists > 0 and aas_prob_0_1 == 0 else "_no_season_assists_or_low_xg" if aas_prob_0_1 == 0 else "")
    return round(ags_prob_0_1 * 100.0, 2), ags_src, round(aas_prob_0_1 * 100.0, 2), aas_src

def calculate_all_matches_combined_stats_with_cs() -> List[Dict[str, Any]]:
    if PLAYER_STATS_DF is None or not ALL_BASE_FIXTURES: print("ERROR (CombinedCalc): Player stats DF or base fixtures not loaded."); return []
    all_match_output_stats: List[Dict[str, Any]] = []
    for fixture_index, fixture in enumerate(ALL_BASE_FIXTURES):
        home_c, away_c, date_s, fixture_id, gw = fixture['home_team_canonical'], fixture['away_team_canonical'], fixture['date_str'], fixture['fixture_id'], fixture['GW']
        current_match_players_data_list, processed_players_tracker = [], set()
        home_xg, away_xg, xg_source_str = None, None, "source_unknown"
        cs_odds_match = CS_ODDS_LOOKUP.get((home_c, away_c, date_s))
        if not cs_odds_match:
            cs_odds_match_rev = CS_ODDS_LOOKUP.get((away_c, home_c, date_s))
            if cs_odds_match_rev: temp_away_xg, temp_home_xg = calculate_xg_from_cs_odds_for_app2(cs_odds_match_rev); home_xg, away_xg = temp_home_xg, temp_away_xg; xg_source_str = "cs_odds_reversed"
        else: home_xg, away_xg = calculate_xg_from_cs_odds_for_app2(cs_odds_match); xg_source_str = "cs_odds_direct" if home_xg is not None else xg_source_str
        if home_xg is None or away_xg is None:
            fdr_metrics = FIXTURE_FDR_METRICS_CACHE.get(fixture_id)
            if fdr_metrics and fdr_metrics.get('home_fdr_outright') is not None:
                home_xg, away_xg = estimate_xg_from_fdr_outrights_for_app2(fdr_metrics['home_fdr_outright'], fdr_metrics['away_fdr_outright']); xg_source_str = "fdr_outrights_estimation"
            else: home_xg, away_xg = AVERAGE_TOTAL_GOALS_IN_MATCH / 2.0, AVERAGE_TOTAL_GOALS_IN_MATCH / 2.0; xg_source_str = "default_average_fallback"
        home_xg, away_xg = float(home_xg or 0), float(away_xg or 0)
        
        team_cs_home, team_cs_away = 0.0, 0.0
        cs_cache_key = FIXTURE_ID_TO_CS_CACHE_KEY_MAP.get(fixture_id)
        if cs_cache_key and cs_cache_key in TEAM_CS_PERCENTAGES_CACHE:
            cs_data_match = TEAM_CS_PERCENTAGES_CACHE[cs_cache_key]
            team_cs_home, team_cs_away = cs_data_match.get(home_c, 0.0), cs_data_match.get(away_c, 0.0)

        if PLAYER_STATS_DF is not None and not PLAYER_STATS_DF.empty:
            for team_c_loop, is_home in [(home_c, True), (away_c, False)]:
                team_xg = home_xg if is_home else away_xg
                players_df_team = PLAYER_STATS_DF[PLAYER_STATS_DF['Team_Canonical'] == team_c_loop]
                team_goals_total, team_assists_total = TEAM_SEASON_STATS.get(team_c_loop, {}).get("goals", 0.0), TEAM_SEASON_STATS.get(team_c_loop, {}).get("assists", 0.0)
                for _, p_row in players_df_team.iterrows():
                    p_name, p_id, p_api_id = str(p_row.get('Player Name','N/A')), str(p_row.get('player_id')) if pd.notna(p_row.get('player_id')) else None, str(p_row.get('Player API ID')) if pd.notna(p_row.get('Player API ID')) else None
                    ags_p, ags_s, aas_p, aas_s = _calculate_player_specific_probabilities_for_app2(p_name, p_id, p_api_id, team_c_loop, float(p_row.get('Goals',0.0)), float(p_row.get('Assists',0.0)), team_goals_total, team_assists_total, team_xg, home_c, away_c, date_s, DIRECT_AGS_DATA_FROM_JSON, xg_source_str)
                    p_pos = p_row.get('Position')
                    p_cs_prob = 0.0
                    if p_pos and any(def_p.lower() in str(p_pos).lower() for def_p in DEFENSIVE_POSITIONS):
                        p_cs_prob = team_cs_home if is_home else team_cs_away
                    p_team_details = TEAM_DETAILS.get(team_c_loop, DEFAULT_TEAM_DETAIL)
                    current_match_players_data_list.append({
                        "player_name": p_name, "player_id": p_id, "player_api_id": p_api_id, "team_name_canonical": team_c_loop,
                        "team_api_id": p_team_details.get('api_id'), "team_short_code": p_team_details['short_code'], "Position": p_pos,
                        "player_display_name": p_row.get('player_display_name', p_name), "player_price": p_row.get('player_price'), "player_image": p_row.get('player_image'),
                        "anytime_goalscorer_probability": ags_p, "ags_prob_source": ags_s, "anytime_assist_probability": aas_p, "aas_prob_source": aas_s,
                        "clean_sheet_probability": round(p_cs_prob, 2)
                    })
                    processed_players_tracker.add((p_name.lower(), team_c_loop, p_id, p_api_id))
        
        if DIRECT_AGS_DATA_FROM_JSON and 'matches' in DIRECT_AGS_DATA_FROM_JSON:
            for ags_match in DIRECT_AGS_DATA_FROM_JSON['matches']:
                entry_home_ags, entry_away_ags, entry_date_ags = get_canonical_team_name(ags_match.get('home_team',''), TEAM_NAME_MAPPING), get_canonical_team_name(ags_match.get('away_team',''), TEAM_NAME_MAPPING), ags_match.get('date')
                if frozenset({entry_home_ags, entry_away_ags}) == frozenset({home_c, away_c}) and entry_date_ags == date_s:
                    for ags_p_json in ags_match.get('players',[]):
                        p_name_j, p_team_orig_j = str(ags_p_json.get('player','')).strip(), str(ags_p_json.get('team','')).strip()
                        p_team_c_j = get_canonical_team_name(p_team_orig_j, TEAM_NAME_MAPPING)
                        p_id_j, p_api_id_j = str(ags_p_json.get('player_id')) if pd.notna(ags_p_json.get('player_id')) else None, str(ags_p_json.get('player_api_id')) if pd.notna(ags_p_json.get('player_api_id')) else None
                        if not p_name_j or not p_team_c_j or p_team_c_j.startswith("N/A_"): continue
                        
                        already_processed_flag = False
                        for proc_key in processed_players_tracker:
                            proc_n, proc_t, proc_pid, proc_apiid = proc_key
                            if p_name_j.lower() == proc_n and p_team_c_j == proc_t and \
                               ((p_id_j and p_id_j == proc_pid) or (p_api_id_j and p_api_id_j == proc_apiid) or \
                                (not p_id_j and not p_api_id_j and not proc_pid and not proc_apiid)):
                                already_processed_flag = True; break
                        if already_processed_flag: continue
                        
                        direct_ags_p_j = None
                        try: odds_j = float(ags_p_json.get('odds')); direct_ags_p_j = (1.0/odds_j) if odds_j > 1.0 else None
                        except (ValueError,TypeError): pass
                        
                        p_pos_j = ags_p_json.get("position")
                        p_cs_prob_j = 0.0
                        if p_pos_j and any(def_p.lower() in str(p_pos_j).lower() for def_p in DEFENSIVE_POSITIONS):
                            p_cs_prob_j = team_cs_home if p_team_c_j == home_c else team_cs_away if p_team_c_j == away_c else 0.0
                        
                        if direct_ags_p_j is not None:
                            p_team_details_j = TEAM_DETAILS.get(p_team_c_j, DEFAULT_TEAM_DETAIL)
                            current_match_players_data_list.append({
                                "player_name": p_name_j, "player_id": p_id_j, "player_api_id": p_api_id_j, "team_name_canonical": p_team_c_j,
                                "team_api_id": p_team_details_j.get('api_id'), "team_short_code": p_team_details_j['short_code'], "Position": p_pos_j,
                                "player_display_name": p_name_j, "player_price": None, "player_image": None,
                                "anytime_goalscorer_probability": round(direct_ags_p_j * 100.0, 2), "ags_prob_source": "direct_odds_only (not_in_excel)",
                                "anytime_assist_probability": 0.0, "aas_prob_source": "unavailable (not_in_excel)",
                                "clean_sheet_probability": round(p_cs_prob_j, 2)
                            })
                            processed_players_tracker.add((p_name_j.lower(), p_team_c_j, p_id_j, p_api_id_j))

        all_match_output_stats.append({"fixture_id": fixture_id, "GW": gw, "date_str": date_s, "home_team_canonical": home_c, "away_team_canonical": away_c, "home_team_xg": round(home_xg,3), "away_team_xg": round(away_xg,3), "xg_source": xg_source_str, "players_data": current_match_players_data_list})
    return all_match_output_stats

# --- Lifespan Event Handler ---
@asynccontextmanager
async def lifespan_manager(app_instance: FastAPI): 
    print("INFO:     Application startup - Precomputing all data...")
    global PLAYER_STATS_DF, TEAM_SEASON_STATS, CS_ODDS_LOOKUP, DIRECT_AGS_DATA_FROM_JSON, \
           ALL_BASE_FIXTURES, FIXTURE_ID_GW_LOOKUP, TEAM_STRENGTH_METRICS, \
           MATCH_HISTORY_CONTEXTS, FIXTURE_FDR_METRICS_CACHE, \
           FIXTURE_LOOKUP_MAP, TEAM_CS_PERCENTAGES_CACHE, FIXTURE_ID_TO_CS_CACHE_KEY_MAP

    for team_name_detail_key, details_val in TEAM_DETAILS.items():
        if team_name_detail_key not in TEAM_NAME_MAPPING: TEAM_NAME_MAPPING[team_name_detail_key] = team_name_detail_key
        if isinstance(details_val.get("api_id"), int): TEAM_NAME_MAPPING[str(details_val["api_id"])] = team_name_detail_key
    print("INFO:     TEAM_NAME_MAPPING enriched.")

    try: load_and_prepare_fixture_data_for_app1_lookup(FULL_FIXTURE_DATA_RAW, TEAM_NAME_MAPPING)
    except Exception as e: print(f"ERROR: Failed App1 lookup map: {e}")

    try:
        cs_data_cache = load_json_data(CORRECT_SCORE_FILE_PATH)
        if cs_data_cache:
            team_cs_res = calculate_team_cs_percentages_logic(cs_data_cache, TEAM_NAME_MAPPING, TEAM_DETAILS, FIXTURE_LOOKUP_MAP)
            for item in team_cs_res:
                match_id_k, team_c, cs_p, fix_id = item['match_identifier'], item['team_name_canonical'], item['clean_sheet_percentage'], item['fixture_id']
                if match_id_k not in TEAM_CS_PERCENTAGES_CACHE: TEAM_CS_PERCENTAGES_CACHE[match_id_k] = {}
                TEAM_CS_PERCENTAGES_CACHE[match_id_k][team_c] = cs_p
                if fix_id and fix_id != "N/A_FID": FIXTURE_ID_TO_CS_CACHE_KEY_MAP[fix_id] = match_id_k
            print(f"INFO:     Team CS percentages cached ({len(TEAM_CS_PERCENTAGES_CACHE)} matches). FIXTURE_ID_TO_CS_CACHE_KEY_MAP populated ({len(FIXTURE_ID_TO_CS_CACHE_KEY_MAP)}).")
        else: print("ERROR:    Correct score data file not found for CS cache.")
    except Exception as e: print(f"ERROR: Failed pre-caching team CS percentages: {e}")

    try: _populate_fixture_id_gw_lookup_for_app2(FULL_FIXTURE_DATA_RAW, TEAM_NAME_MAPPING)
    except Exception as e: print(f"ERROR: App2 FIXTURE_ID_GW_LOOKUP population: {e}")
    try:
        create_base_fixtures_with_canonical_names_from_hardcoded_for_app2(USER_PROVIDED_FIXTURES_WITH_STADIUMS_RAW, TEAM_NAME_MAPPING, FIXTURE_ID_GW_LOOKUP)
        if not ALL_BASE_FIXTURES: print("CRITICAL WARNING: ALL_BASE_FIXTURES empty.")
    except Exception as e: print(f"ERROR: App2 ALL_BASE_FIXTURES creation: {e}")

    all_teams_app2 = {team_c for fix in ALL_BASE_FIXTURES for team_c in (fix['home_team_canonical'], fix['away_team_canonical'])} if ALL_BASE_FIXTURES else set()
    for team_c in all_teams_app2:
        if team_c not in TEAM_SEASON_STATS: TEAM_SEASON_STATS[team_c] = {"goals": 0.0, "assists": 0.0}
    print(f"INFO:     Initialized TEAM_SEASON_STATS for {len(all_teams_app2)} teams.")

    try:
        PLAYER_STATS_DF = pd.read_excel(PLAYER_STATS_FP, sheet_name='Sheet1')
        team_col = 'Team Name' if 'Team Name' in PLAYER_STATS_DF.columns else 'Team' if 'Team' in PLAYER_STATS_DF.columns else None
        if not team_col: raise ValueError(f"Excel {PLAYER_STATS_FP} needs 'Team Name' or 'Team' column.")
        for col in ['Player Name', 'Position', 'player_id', 'Player API ID', 'player_display_name', 'player_price', 'player_image', 'Goals', 'Assists']:
            if col not in PLAYER_STATS_DF.columns: PLAYER_STATS_DF[col] = 0.0 if col in ['Goals','Assists'] else None
        PLAYER_STATS_DF['Team_Canonical'] = PLAYER_STATS_DF[team_col].apply(lambda x: get_canonical_team_name(str(x), TEAM_NAME_MAPPING))
        for col in ['Goals', 'Assists']: PLAYER_STATS_DF[col] = pd.to_numeric(PLAYER_STATS_DF[col], errors='coerce').fillna(0.0)
        for team_c, group_df in PLAYER_STATS_DF.groupby('Team_Canonical'):
            if team_c in TEAM_SEASON_STATS: 
                TEAM_SEASON_STATS[team_c]["goals"] = float(group_df['Goals'].sum())
                TEAM_SEASON_STATS[team_c]["assists"] = float(group_df['Assists'].sum())
        print(f"INFO:     PLAYER_STATS_DF loaded, TEAM_SEASON_STATS updated.")
    except Exception as e: print(f"ERROR: Loading player stats {PLAYER_STATS_FP}: {e}"); PLAYER_STATS_DF = pd.DataFrame()

    cs_data_app2_xg = load_json_data(CORRECT_SCORE_FILE_PATH) 
    if cs_data_app2_xg and 'matches' in cs_data_app2_xg:
        for match_entry in cs_data_app2_xg['matches']:
            match_s, cs_o, date_v = match_entry.get('match',''), match_entry.get('correct_score_odds'), match_entry.get('date')
            h_c, a_c = parse_cs_match_string_for_canonical_teams_for_app2(match_s, TEAM_NAME_MAPPING)
            if h_c and a_c and date_v and isinstance(cs_o, dict): CS_ODDS_LOOKUP[(h_c, a_c, date_v)] = cs_o
        print(f"INFO:     CS_ODDS_LOOKUP populated ({len(CS_ODDS_LOOKUP)}).")

    try:
        DIRECT_AGS_DATA_FROM_JSON = load_json_data(UNIFIED_ANYTIME_GOALSCORER_FILE_PATH)
        if DIRECT_AGS_DATA_FROM_JSON: print("INFO:     Direct AGS JSON data loaded.")
    except Exception as e: print(f"ERROR: Loading Direct AGS JSON: {e}")

    try:
        df_outright = get_tournament_outright_odds_data_for_app2(HTML_ODDS_FP, MD_ODDS_FP, TEAM_NAME_MAPPING)
        normalize_tournament_implied_probs_for_app2(df_outright, all_teams_app2)
    except Exception as e: print(f"ERROR: TEAM_STRENGTH_METRICS calculation: {e}"); TEAM_STRENGTH_METRICS = {t:10.0 for t in all_teams_app2}

    if ALL_BASE_FIXTURES:
        ALL_BASE_FIXTURES.sort(key=lambda x: x['datetime_obj']) 
        try: create_last_match_dates_history_for_app2(ALL_BASE_FIXTURES)
        except Exception as e: print(f"ERROR: MATCH_HISTORY_CONTEXTS creation: {e}")

    if ALL_BASE_FIXTURES and TEAM_STRENGTH_METRICS and MATCH_HISTORY_CONTEXTS:
        try:
            for i, fix_fdr in enumerate(ALL_BASE_FIXTURES):
                hist_ctx = MATCH_HISTORY_CONTEXTS[i] if i < len(MATCH_HISTORY_CONTEXTS) else {}
                calculate_outright_fdr_components_for_app2(fix_fdr, TEAM_STRENGTH_METRICS, hist_ctx)
            print(f"INFO:     FIXTURE_FDR_METRICS_CACHE populated ({len(FIXTURE_FDR_METRICS_CACHE)}).")
        except Exception as e: 
            print(f"ERROR:    Failed during App2 FIXTURE_FDR_METRICS_CACHE population: {e}")
            import traceback
            traceback.print_exc()


    print("INFO:     Application startup precomputation complete.")
    yield
    print("INFO:     Application shutdown.")

app = FastAPI(
    title="Football Super Stats API",
    description="API for Team/Player Clean Sheets, Top Correct Scores, and Combined Player Stats (AGS, AAS, CS).",
    version="2.1.2", # Incremented for these fixes
    lifespan=lifespan_manager 
)

# --- FastAPI Endpoints ---
@app.get("/team-clean-sheets/", response_model=List[TeamCleanSheet], tags=["Clean Sheets & Scores (Original)"])
async def get_team_clean_sheets():
    try:
        cs_data = load_json_data(CORRECT_SCORE_FILE_PATH)
        if not cs_data: raise HTTPException(status_code=500, detail="Could not load correct_score.json")
        results = calculate_team_cs_percentages_logic(cs_data, TEAM_NAME_MAPPING, TEAM_DETAILS, FIXTURE_LOOKUP_MAP)
        if not results: raise HTTPException(status_code=404, detail="No team clean sheet data calculated.")
        return results
    except Exception as e: 
        print(f"ERROR /team-clean-sheets/: {e}"); import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/top-correct-scores/", response_model=List[TopCorrectScores], tags=["Clean Sheets & Scores (Original)"])
async def get_top_correct_scores():
    try:
        cs_data = load_json_data(CORRECT_SCORE_FILE_PATH)
        if not cs_data: raise HTTPException(status_code=500, detail="Could not load correct_score.json")
        results = calculate_top_scores_logic(cs_data, TEAM_NAME_MAPPING, FIXTURE_LOOKUP_MAP)
        if not results: raise HTTPException(status_code=404, detail="No top correct score data calculated.")
        return results
    except Exception as e: 
        print(f"ERROR /top-correct-scores/: {e}"); import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/player-clean-sheets/", response_model=List[MatchWithPlayerCleanSheets], tags=["Clean Sheets & Scores (Original)"])
async def get_player_clean_sheets():
    try:
        ags_data = load_json_data(UNIFIED_ANYTIME_GOALSCORER_FILE_PATH)
        if not ags_data: raise HTTPException(status_code=500, detail="Could not load anytime_goalscorer.json")
        if not TEAM_CS_PERCENTAGES_CACHE: raise HTTPException(status_code=503, detail="Team CS cache unavailable.")
        results = calculate_player_clean_sheets_logic(ags_data, TEAM_CS_PERCENTAGES_CACHE, TEAM_NAME_MAPPING, TEAM_DETAILS, FIXTURE_LOOKUP_MAP)
        if not results: raise HTTPException(status_code=404, detail="No player clean sheet data calculated.")
        return results
    except Exception as e: 
        print(f"ERROR /player-clean-sheets/: {e}"); import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# Merged Endpoint
@app.get("/all-matches-player-stats/", response_model=List[MatchWithPlayerCombinedStats], tags=["Player Stats (Combined)"])
async def get_all_matches_player_combined_stats_endpoint():
    try:
        all_matches_data = calculate_all_matches_combined_stats_with_cs() 
        if not all_matches_data: raise HTTPException(status_code=404, detail="No combined player stats calculated.")
        results = [MatchWithPlayerCombinedStats(
                        fixture_id=match_data["fixture_id"], GW=match_data["GW"], date_str=match_data["date_str"],
                        home_team_canonical=match_data["home_team_canonical"], away_team_canonical=match_data["away_team_canonical"],
                        home_team_xg=match_data.get("home_team_xg"), away_team_xg=match_data.get("away_team_xg"),
                        xg_source=match_data.get("xg_source"),
                        players=[PlayerCombinedStats(**p_data) for p_data in match_data["players_data"]]
                    ) for match_data in all_matches_data]
        if not results: raise HTTPException(status_code=404, detail="Combined player stats results list empty after processing.")
        return results
    except Exception as e: 
        print(f"Error /all-matches-player-stats/: {e}"); import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    import traceback 
    print("Running script directly for testing & output generation...")
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR); print(f"INFO: Created data directory: {DATA_DIR}")
    else: print(f"INFO: Using existing data directory: {DATA_DIR}")

    async def main_script_runner():
        async with lifespan_manager(app) as _: 
            print("Lifespan simulation complete. Data loaded.")
            print("\n--- Testing New Combined Player Stats Logic & Generating Output ---")
            output_combined = calculate_all_matches_combined_stats_with_cs()
            print(f"Combined Player Stats: Processed {len(output_combined)} matches.")
            if output_combined:
                try:
                    with open(OUTPUT_COMBINED_PLAYER_STATS_JSON_FP, 'w', encoding='utf-8') as f:
                        json.dump(output_combined, f, indent=2, ensure_ascii=False)
                    print(f"Combined player stats output saved to {OUTPUT_COMBINED_PLAYER_STATS_JSON_FP}")
                except Exception as e_json: print(f"Error saving output to JSON: {e_json}")
            else: print("No combined stats generated, skipping file output.")

    import asyncio
    if os.name == 'nt': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_script_runner())
    print("\n--- Testing Complete ---")
    print("To run as an API server (e.g., if file is main.py): uvicorn main:app --reload --port 8000")
# To run this application:
# 1. Save this script as main.py
# 2. Make sure correct_score.json and updated_anytimegoalscorer.json are in the same directory.
# 3. Open your terminal in this directory and run:
#    uvicorn main:app --reload
#
# Then you can access the APIs at:
# http://127.0.0.1:8000/team-clean-sheets/
# http://127.0.0.1:8000/top-correct-scores/
# http://127.0.0.1:8000/player-clean-sheets/
# And the auto-generated API docs at: http://127.0.0.1:8000/docs