import json
from fastapi import FastAPI, HTTPException
from typing import List, Dict, Any, FrozenSet, Optional
import io
import csv
from contextlib import asynccontextmanager
from pydantic import BaseModel

# --- Team Name Mapping (for standardizing names from various sources) ---
TEAM_NAME_MAPPING = {
    "Real Madrid": "Real Madrid CF", "Manchester City": "Manchester City FC",
    "Bayern Munich": "FC Bayern München", "PSG": "Paris Saint-Germain",
    "Inter": "FC Internazionale Milano", "Chelsea": "Chelsea FC",
    "Atl. Madrid": "Atlético de Madrid", "Dortmund": "Borussia Dortmund",
    "Juventus": "Juventus FC", "FC Porto": "FC Porto",
    "Flamengo RJ": "CR Flamengo", "Benfica": "SL Benfica",
    "Palmeiras": "SE Palmeiras", "Boca Juniors": "CA Boca Juniors",
    "River Plate": "CA River Plate", "Botafogo RJ": "Botafogo FR",
    "Fluminense": "Fluminense FC", "Al Hilal": "Al Hilal SFC",
    "Inter Miami": "Inter Miami CF", "Salzburg": "FC Salzburg",
    "Los Angeles FC": "LAFC", "Seattle Sounders": "Seattle Sounders FC",
    "Al Ahly": "Al Ahly FC", "Pachuca": "CF Pachuca",
    "Urawa Reds": "Urawa Red Diamonds", "Ulsan Hyundai": "Ulsan HD FC",
    "Al Ain": "Al Ain FC", "Monterrey": "CF Monterrey",
    "Esperance Tunis": "Espérance Sportive de Tunis", "ES Tunis": "Espérance Sportive de Tunis",
    "Wydad Athletic": "Wydad AC", "Wydad Casablanca": "Wydad AC",
    "Mamelodi Sundowns": "Mamelodi Sundowns FC", "Auckland City": "Auckland City FC",

    # Canonical names (mapping to themselves for completeness)
    "Real Madrid CF": "Real Madrid CF", "Manchester City FC": "Manchester City FC",
    "FC Bayern München": "FC Bayern München", "Paris Saint-Germain": "Paris Saint-Germain",
    "Paris Saint Germain": "Paris Saint-Germain", 
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

    "Al Ahly SC": "Al Ahly FC", "Paris SG": "Paris Saint-Germain",
    "Atletico Madrid": "Atlético de Madrid", 
    "Man City": "Manchester City FC", "Bayern": "FC Bayern München",
    "Inter Milan": "FC Internazionale Milano", "Atl Madrid": "Atlético de Madrid",
    "Porto": "FC Porto",
    "Botafogo": "Botafogo FR", 
    "Flamengo": "CR Flamengo", 
    "Ulsan HD": "Ulsan HD FC", 
    "Regatas Flamengo RJ": "CR Flamengo", 
    "CA River Plate BA": "CA River Plate", 
    "Ulsan Hyundai FC": "Ulsan HD FC", 
    "Al Ain Abu Dhabi": "Al Ain FC", 
    "Wydad AC Casablanca": "Wydad AC", 
}

# --- Updated Team Details (Short Code, API ID, and new team_id) ---
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

FIXTURE_LOOKUP_MAP: Dict[FrozenSet[str], Dict[str, Any]] = {} 
TEAM_CS_PERCENTAGES_CACHE: Dict[str, Dict[str, float]] = {} 

CORRECT_SCORE_FILE_PATH = "correct_score.json"
ANYTIME_GOALSCORER_FILE_PATH = "updated_anytimegoalscorer.json"

DEFENSIVE_POSITIONS = [
    "Goalkeeper", "Centre-Back", "Left-Back", "Right-Back", "Sweeper"
]

# --- Pydantic Models ---
class TeamCleanSheet(BaseModel):
    match_identifier: str
    fixture_id: str
    GW: str
    team_id: str
    team_name_original: str
    team_name_canonical: str
    short_code: str
    api_id: Any
    clean_sheet_percentage: float
    image_url: str

class TopCorrectScoreItem(BaseModel):
    score: str
    percentage: Any

class TopCorrectScores(BaseModel):
    match_identifier: str
    fixture_id: str
    GW: str
    top_scores: List[TopCorrectScoreItem]

class DefensivePlayerCleanSheetInfo(BaseModel): # Renamed from PlayerCleanSheet
    player_name: str
    player_id: Optional[str] = None
    player_api_id: Optional[Any] = None
    team_name_canonical: str
    team_id: str
    team_short_code: str
    team_api_id: Any
    position: str
    clean_sheet_percentage: float
    team_image_url: str

class MatchWithPlayerCleanSheets(BaseModel): # New model for the desired output structure
    match_identifier: str
    fixture_id: str
    GW: str
    defensive_players: List[DefensivePlayerCleanSheetInfo]


# --- Lifespan Event Handler ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("INFO:     Application startup...")
    for team_name_detail_key in TEAM_DETAILS.keys():
        if team_name_detail_key not in TEAM_NAME_MAPPING:
            TEAM_NAME_MAPPING[team_name_detail_key] = team_name_detail_key
        if isinstance(TEAM_DETAILS[team_name_detail_key].get("api_id"), int):
            TEAM_NAME_MAPPING[str(TEAM_DETAILS[team_name_detail_key]["api_id"])] = team_name_detail_key

    full_fixture_data_raw = """fixture_id	stage_name	starting_at	home_team_name	away_team_name	group_name	home_team_id	away_team_id	GW
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
    load_and_prepare_fixture_data_from_string(full_fixture_data_raw, TEAM_NAME_MAPPING)
    print(f"INFO:     Fixture lookup map populated with {len(FIXTURE_LOOKUP_MAP)} entries from string data.")

    try:
        correct_score_data = load_data_from_file(CORRECT_SCORE_FILE_PATH)
        team_cs_results = calculate_team_cs_percentages_logic(
            correct_score_data, TEAM_NAME_MAPPING, TEAM_DETAILS, FIXTURE_LOOKUP_MAP
        )
        for item in team_cs_results:
            match_id = item['match_identifier'] 
            team_canon = item['team_name_canonical']
            cs_perc = item['clean_sheet_percentage']
            if match_id not in TEAM_CS_PERCENTAGES_CACHE:
                TEAM_CS_PERCENTAGES_CACHE[match_id] = {}
            TEAM_CS_PERCENTAGES_CACHE[match_id][team_canon] = cs_perc
        print(f"INFO:     Team CS percentages cached for {len(TEAM_CS_PERCENTAGES_CACHE)} matches.")
    except Exception as e:
        print(f"ERROR:    Failed to pre-calculate and cache team CS percentages: {e}")
    yield
    print("INFO:     Application shutdown.")

app = FastAPI(
    title="Football Stats API",
    description="API to calculate team/player clean sheet percentages and top correct scores.",
    version="1.5.0", # Incremented version
    lifespan=lifespan
)

# --- Helper Functions (load_data_from_file, get_canonical_team_name_api, load_and_prepare_fixture_data_from_string) ---
# ... (These functions remain the same as in the previous version) ...
def load_data_from_file(file_path: str) -> Dict[str, Any]:
    """Loads JSON data from the specified file path."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail=f"Data file '{file_path}' not found on server.")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail=f"Error decoding JSON from '{file_path}'.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error loading data from '{file_path}': {e}")

def get_canonical_team_name_api(name_from_source: str, mapping: Dict[str, str]) -> str:
    """Gets the canonical team name using the provided mapping. Enhanced for robustness."""
    name_from_source_stripped = name_from_source.strip()

    if not name_from_source_stripped:
        return "N/A_EmptyName"

    if name_from_source_stripped in mapping:
        return mapping[name_from_source_stripped]
    if name_from_source in mapping: 
        return mapping[name_from_source]

    if name_from_source_stripped in mapping.values():
        return name_from_source_stripped
    if name_from_source in mapping.values():
        return name_from_source

    name_lower = name_from_source_stripped.lower()
    for map_key, canonical_val in mapping.items():
        if name_lower == map_key.lower():
            return canonical_val

    suffixes_to_remove = [" fc", " cf", " rj", " fr", " hd", " sc", " ac", " c.f.", " c. f.", " c f", " ba", " münchen"] 
    punctuation_to_remove = ".()" 

    temp_name_norm = name_lower
    for suffix in suffixes_to_remove:
        temp_name_norm = temp_name_norm.replace(suffix, "")
    for punc in punctuation_to_remove:
        temp_name_norm = temp_name_norm.replace(punc, "")
    temp_name_norm = temp_name_norm.strip() 

    for map_key, canonical_val in mapping.items():
        map_key_norm = map_key.lower()
        for suffix in suffixes_to_remove:
            map_key_norm = map_key_norm.replace(suffix, "")
        for punc in punctuation_to_remove:
            map_key_norm = map_key_norm.replace(punc, "")
        map_key_norm = map_key_norm.strip()

        if temp_name_norm == map_key_norm:
            return canonical_val

        canonical_val_norm = canonical_val.lower()
        for suffix in suffixes_to_remove:
            canonical_val_norm = canonical_val_norm.replace(suffix, "")
        for punc in punctuation_to_remove:
            canonical_val_norm = canonical_val_norm.replace(punc, "")
        canonical_val_norm = canonical_val_norm.strip()

        if temp_name_norm == canonical_val_norm:
            return canonical_val
    
    if name_from_source_stripped.isdigit():
        if name_from_source_stripped in mapping: 
             return mapping[name_from_source_stripped]

    return name_from_source_stripped


def load_and_prepare_fixture_data_from_string(raw_data_string: str, team_mapping: Dict[str, str]):
    global FIXTURE_LOOKUP_MAP
    FIXTURE_LOOKUP_MAP = {}
    data_io = io.StringIO(raw_data_string)
    reader = csv.reader(data_io, delimiter='\t')
    try:
        header = next(reader) 
    except StopIteration:
        print("Error: Fixture data string is empty or header is missing.")
        return

    for i, row in enumerate(reader):
        if len(row) < 9:
            continue
        stage_name = row[1].strip()
        if "Group Stage" not in stage_name:
            continue
        fixture_id_fixture = row[0].strip()
        home_team_original_fixture = row[3].strip()
        away_team_original_fixture = row[4].strip()
        gw_fixture = row[8].strip()
        fixture_datetime_str = row[2].strip() 
        fixture_date_fixture = fixture_datetime_str.split(" ")[0] if fixture_datetime_str else "N/A"
        group_fixture = row[5].strip()

        if not home_team_original_fixture or not away_team_original_fixture or \
           "Winner Match" in home_team_original_fixture or "1st Group" in home_team_original_fixture or \
           "Winner Match" in away_team_original_fixture or "2nd Group" in away_team_original_fixture:
            continue 
        canonical_home_fixture = get_canonical_team_name_api(home_team_original_fixture, team_mapping)
        canonical_away_fixture = get_canonical_team_name_api(away_team_original_fixture, team_mapping)
        map_key = frozenset({canonical_home_fixture, canonical_away_fixture})
        fixture_details_to_store = {
            "fixture_id": fixture_id_fixture,
            "GW": gw_fixture,
            "fixture_date": fixture_date_fixture, 
            "group": group_fixture, 
            "_fixture_original_home": home_team_original_fixture,
            "_fixture_original_away": away_team_original_fixture,
            "_canonical_home_fixture": canonical_home_fixture, 
            "_canonical_away_fixture": canonical_away_fixture, 
        }
        if map_key in FIXTURE_LOOKUP_MAP:
            pass
        FIXTURE_LOOKUP_MAP[map_key] = fixture_details_to_store

# --- Calculation Logic Functions ---
def calculate_team_cs_percentages_logic(
    correct_score_data: Dict[str, Any],
    team_mapping: Dict[str, str],
    team_details_map: Dict[str, Dict[str, Any]],
    fixture_lookup: Dict[FrozenSet[str], Dict[str, Any]]
) -> List[Dict[str, Any]]:
    if not correct_score_data or 'matches' not in correct_score_data:
        return []
    team_clean_sheet_rows = []
    for match_info_from_json in correct_score_data['matches']:
        match_str_json = match_info_from_json.get('match')
        odds_dict_json = match_info_from_json.get('correct_score_odds')
        date_json = match_info_from_json.get('date', 'N/A') 
        stadium_json = match_info_from_json.get('stadium', 'N/A')
        if not match_str_json or not odds_dict_json:
            continue
        try:
            team_names_from_json = match_str_json.split(" vs ")
            if len(team_names_from_json) != 2:
                continue
            home_team_original_json = team_names_from_json[0].strip()
            away_team_original_json = team_names_from_json[1].strip()
            home_team_canonical_json = get_canonical_team_name_api(home_team_original_json, team_mapping)
            away_team_canonical_json = get_canonical_team_name_api(away_team_original_json, team_mapping)
        except Exception as e:
            continue
        lookup_key = frozenset({home_team_canonical_json, away_team_canonical_json})
        fixture_data_from_map = fixture_lookup.get(lookup_key)
        fixture_id = "N/A"
        gw = "N/A"
        if fixture_data_from_map:
            fixture_id = fixture_data_from_map.get("fixture_id", "N/A")
            gw = fixture_data_from_map.get("GW", "N/A")
        sum_implied_probs_all_scores = 0.0
        sum_implied_probs_home_cs = 0.0
        sum_implied_probs_away_cs = 0.0
        if not isinstance(odds_dict_json, dict):
            continue
        for score_str, odd_value in odds_dict_json.items():
            try:
                odd = float(odd_value)
                if odd <= 0: implied_prob = 0.0
                else: implied_prob = 1.0 / odd
                sum_implied_probs_all_scores += implied_prob
                score_parts = score_str.split('-')
                if len(score_parts) != 2: continue
                home_goals, away_goals = int(score_parts[0]), int(score_parts[1])
                if away_goals == 0: sum_implied_probs_home_cs += implied_prob
                if home_goals == 0: sum_implied_probs_away_cs += implied_prob
            except (ValueError, ZeroDivisionError, TypeError) as e:
                continue
        home_cs_percentage = (sum_implied_probs_home_cs / sum_implied_probs_all_scores * 100) if sum_implied_probs_all_scores > 0 else 0.0
        away_cs_percentage = (sum_implied_probs_away_cs / sum_implied_probs_all_scores * 100) if sum_implied_probs_all_scores > 0 else 0.0
        correct_score_match_identifier = f"{match_str_json} ({date_json} at {stadium_json})"
        home_details = team_details_map.get(home_team_canonical_json, {"team_id": "N/A", "short_code": "N/A", "api_id": "N/A", "image": "N/A"})
        away_details = team_details_map.get(away_team_canonical_json, {"team_id": "N/A", "short_code": "N/A", "api_id": "N/A", "image": "N/A"})
        team_clean_sheet_rows.append({
            'match_identifier': correct_score_match_identifier, 
            'fixture_id': fixture_id, 'GW': gw, 'team_id': home_details["team_id"],
            'team_name_original': home_team_original_json, 'team_name_canonical': home_team_canonical_json,
            'short_code': home_details["short_code"], 'api_id': home_details["api_id"],
            'clean_sheet_percentage': round(home_cs_percentage, 2), 'image_url': home_details["image"]
        })
        team_clean_sheet_rows.append({
            'match_identifier': correct_score_match_identifier, 
            'fixture_id': fixture_id, 'GW': gw, 'team_id': away_details["team_id"],
            'team_name_original': away_team_original_json, 'team_name_canonical': away_team_canonical_json,
            'short_code': away_details["short_code"], 'api_id': away_details["api_id"],
            'clean_sheet_percentage': round(away_cs_percentage, 2), 'image_url': away_details["image"]
        })
    return team_clean_sheet_rows

def calculate_top_scores_logic(
    correct_score_data: Dict[str, Any],
    team_mapping: Dict[str, str],
    fixture_lookup: Dict[FrozenSet[str], Dict[str, Any]]
) -> List[Dict[str, Any]]:
    if not correct_score_data or 'matches' not in correct_score_data:
        return []
    top_scores_output = []
    for match_info_from_json in correct_score_data['matches']:
        match_str_json = match_info_from_json.get('match')
        odds_dict_json = match_info_from_json.get('correct_score_odds')
        date_json = match_info_from_json.get('date', 'N/A')
        stadium_json = match_info_from_json.get('stadium', 'N/A')
        if not match_str_json or not odds_dict_json or not isinstance(odds_dict_json, dict) or not odds_dict_json:
            continue
        try:
            team_names_from_json = match_str_json.split(" vs ")
            if len(team_names_from_json) != 2:
                continue
            home_team_original_json = team_names_from_json[0].strip()
            away_team_original_json = team_names_from_json[1].strip()
            home_team_canonical_json = get_canonical_team_name_api(home_team_original_json, team_mapping)
            away_team_canonical_json = get_canonical_team_name_api(away_team_original_json, team_mapping)
        except Exception:
            continue
        lookup_key = frozenset({home_team_canonical_json, away_team_canonical_json})
        fixture_data_from_map = fixture_lookup.get(lookup_key)
        fixture_id = "N/A"
        gw = "N/A"
        if fixture_data_from_map:
            fixture_id = fixture_data_from_map.get("fixture_id", "N/A")
            gw = fixture_data_from_map.get("GW", "N/A")
        correct_score_match_identifier = f"{match_str_json} ({date_json} at {stadium_json})"
        score_probabilities = []
        sum_implied_probs_all_scores = 0.0
        for score_str_cs, odd_value_cs in odds_dict_json.items():
            try:
                odd = float(odd_value_cs)
                if odd <= 0: implied_prob = 0.0
                else: implied_prob = 1.0 / odd
                score_probabilities.append({'score': score_str_cs, 'implied_prob': implied_prob})
                sum_implied_probs_all_scores += implied_prob
            except (ValueError, ZeroDivisionError, TypeError):
                continue
        if sum_implied_probs_all_scores == 0:
            top_scores_output.append({
                'match_identifier': correct_score_match_identifier, 'fixture_id': fixture_id, 'GW': gw,
                'top_scores': [{'score': 'N/A', 'percentage': 'N/A (No valid odds)'}]
            })
            continue
        normalized_scores = [{'score': item['score'], 'percentage': (item['implied_prob'] / sum_implied_probs_all_scores) * 100} for item in score_probabilities]
        sorted_scores = sorted(normalized_scores, key=lambda x: x['percentage'], reverse=True)
        match_top_scores = []
        for i in range(min(4, len(sorted_scores))):
             match_top_scores.append({
                'score': sorted_scores[i]['score'],
                'percentage': round(sorted_scores[i]['percentage'], 2)
            })
        top_scores_output.append({
            'match_identifier': correct_score_match_identifier, 'fixture_id': fixture_id, 'GW': gw,
            'top_scores': match_top_scores
            })
    return top_scores_output

def calculate_player_clean_sheets_logic(
    anytime_goalscorer_data: Dict[str, Any],
    team_cs_cache: Dict[str, Dict[str, float]],
    team_mapping: Dict[str, str],
    team_details_map: Dict[str, Dict[str, Any]],
    fixture_lookup: Dict[FrozenSet[str], Dict[str, Any]]
) -> List[Dict[str, Any]]: # Output will be List[MatchWithPlayerCleanSheets]
    if not anytime_goalscorer_data or 'matches' not in anytime_goalscorer_data:
        return []

    # Use a dictionary to group players by match
    # Key: target_match_identifier_in_cache (from correct_score.json's perspective)
    # Value: Dict containing match_info and list of players
    matches_with_players_dict: Dict[str, Dict[str, Any]] = {}

    for ag_match in anytime_goalscorer_data['matches']:
        ag_home_team_original = ag_match.get("home_team")
        ag_away_team_original = ag_match.get("away_team")
        ag_date = ag_match.get("date")
        ag_stadium = ag_match.get("stadium", "N/A")
        ag_players = ag_match.get("players", [])

        if not ag_home_team_original or not ag_away_team_original or not ag_date:
            continue

        ag_home_team_canonical = get_canonical_team_name_api(ag_home_team_original, team_mapping)
        ag_away_team_canonical = get_canonical_team_name_api(ag_away_team_original, team_mapping)
        
        fixture_data_from_map_for_ag_match = fixture_lookup.get(frozenset({ag_home_team_canonical, ag_away_team_canonical}))
        
        current_fixture_id = "N/A"
        current_gw = "N/A"
        target_match_identifier_in_cache = f"{ag_home_team_original} vs {ag_away_team_original} ({ag_date} at {ag_stadium})" # Fallback identifier

        home_cs_perc = 0.0
        away_cs_perc = 0.0
        found_cs_for_match = False

        if fixture_data_from_map_for_ag_match:
            current_fixture_id = fixture_data_from_map_for_ag_match.get("fixture_id", "N/A")
            current_gw = fixture_data_from_map_for_ag_match.get("GW", "N/A")

            # Try to find the exact match_identifier from correct_score.json used in the cache
            for cs_match_id_key, cs_data in team_cs_cache.items():
                try:
                    parts = cs_match_id_key.split(" (")
                    teams_part = parts[0]
                    date_part_stadium_from_cs_key = parts[1].split(" at ")[0] # Date from CS key
                    
                    cs_teams = teams_part.split(" vs ")
                    cs_home_original_from_key = cs_teams[0].strip()
                    cs_away_original_from_key = cs_teams[1].strip()

                    cs_home_canonical_from_key = get_canonical_team_name_api(cs_home_original_from_key, team_mapping)
                    cs_away_canonical_from_key = get_canonical_team_name_api(cs_away_original_from_key, team_mapping)

                    # Match based on canonical teams (order invariant) and date
                    if frozenset({cs_home_canonical_from_key, cs_away_canonical_from_key}) == frozenset({ag_home_team_canonical, ag_away_team_canonical}) and \
                       date_part_stadium_from_cs_key == ag_date:
                        target_match_identifier_in_cache = cs_match_id_key # Use the one from correct_score.json
                        home_cs_perc = cs_data.get(ag_home_team_canonical, 0.0)
                        away_cs_perc = cs_data.get(ag_away_team_canonical, 0.0)
                        found_cs_for_match = True
                        break 
                except Exception:
                    continue
        
        # Initialize match entry in the dictionary if not already present
        if target_match_identifier_in_cache not in matches_with_players_dict:
            matches_with_players_dict[target_match_identifier_in_cache] = {
                "match_identifier": target_match_identifier_in_cache,
                "fixture_id": current_fixture_id,
                "GW": current_gw,
                "defensive_players": []
            }

        for player_data in ag_players:
            player_name = player_data.get("player")
            player_team_original = player_data.get("team")
            player_position = player_data.get("position")

            if not player_name or not player_team_original or not player_position:
                continue

            if player_position in DEFENSIVE_POSITIONS:
                player_team_canonical = get_canonical_team_name_api(player_team_original, team_mapping)
                
                cs_percentage_for_player = 0.0
                if player_team_canonical == ag_home_team_canonical:
                    cs_percentage_for_player = home_cs_perc
                elif player_team_canonical == ag_away_team_canonical:
                    cs_percentage_for_player = away_cs_perc
                
                player_team_details = team_details_map.get(player_team_canonical, {"team_id": "N/A", "short_code": "N/A", "api_id": "N/A", "image": "N/A"})

                player_info = DefensivePlayerCleanSheetInfo(
                    player_name=player_name,
                    player_id=player_data.get("player_id"),
                    player_api_id=player_data.get("player_api_id"),
                    team_name_canonical=player_team_canonical,
                    team_id=player_team_details["team_id"],
                    team_short_code=player_team_details["short_code"],
                    team_api_id=player_team_details["api_id"],
                    position=player_position,
                    clean_sheet_percentage=round(cs_percentage_for_player, 2),
                    team_image_url=player_team_details["image"]
                )
                matches_with_players_dict[target_match_identifier_in_cache]["defensive_players"].append(player_info)
    
    # Convert the dictionary to the desired list output
    return list(matches_with_players_dict.values())


# --- FastAPI Endpoints ---
@app.get("/team-clean-sheets/", response_model=List[TeamCleanSheet], tags=["Stats"])
async def get_team_clean_sheets():
    correct_score_data = load_data_from_file(CORRECT_SCORE_FILE_PATH)
    results = calculate_team_cs_percentages_logic(
        correct_score_data, TEAM_NAME_MAPPING, TEAM_DETAILS, FIXTURE_LOOKUP_MAP
    )
    if not results:
        raise HTTPException(status_code=404, detail="No team clean sheet data could be calculated.")
    return results

@app.get("/top-correct-scores/", response_model=List[TopCorrectScores], tags=["Stats"])
async def get_top_correct_scores():
    correct_score_data = load_data_from_file(CORRECT_SCORE_FILE_PATH)
    results = calculate_top_scores_logic(
        correct_score_data, TEAM_NAME_MAPPING, FIXTURE_LOOKUP_MAP
    )
    if not results:
        raise HTTPException(status_code=404, detail="No top correct score data could be calculated.")
    return results

@app.get("/player-clean-sheets/", response_model=List[MatchWithPlayerCleanSheets], tags=["Stats"]) # Updated response_model
async def get_player_clean_sheets():
    anytime_goalscorer_data = load_data_from_file(ANYTIME_GOALSCORER_FILE_PATH)
    
    if not TEAM_CS_PERCENTAGES_CACHE:
        print("WARN:     TEAM_CS_PERCENTAGES_CACHE was empty. Attempting to populate for /player-clean-sheets/.")
        try:
            correct_score_data = load_data_from_file(CORRECT_SCORE_FILE_PATH)
            team_cs_results = calculate_team_cs_percentages_logic(
                correct_score_data, TEAM_NAME_MAPPING, TEAM_DETAILS, FIXTURE_LOOKUP_MAP
            )
            for item in team_cs_results:
                match_id = item['match_identifier']
                team_canon = item['team_name_canonical']
                cs_perc = item['clean_sheet_percentage']
                if match_id not in TEAM_CS_PERCENTAGES_CACHE:
                    TEAM_CS_PERCENTAGES_CACHE[match_id] = {}
                TEAM_CS_PERCENTAGES_CACHE[match_id][team_canon] = cs_perc
            if not TEAM_CS_PERCENTAGES_CACHE:
                 raise HTTPException(status_code=500, detail="Failed to load team clean sheet data for player calculations.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error preparing team CS data for player CS: {e}")

    results = calculate_player_clean_sheets_logic(
        anytime_goalscorer_data,
        TEAM_CS_PERCENTAGES_CACHE,
        TEAM_NAME_MAPPING,
        TEAM_DETAILS,
        FIXTURE_LOOKUP_MAP
    )
    if not results:
        raise HTTPException(status_code=404, detail="No player clean sheet data could be calculated.")
    return results
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