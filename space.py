import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from sqlalchemy import text
from database import engine


class SpaceDomainDetector:

    def __init__(self):
        self.domain = "Space"

        try:
            # ✅ Load keywords from SPACE table
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT keyword, column_name FROM space_keywords")
                )
                data = result.fetchall()

            self.keywords = [row[0].lower() for row in data if row[0]]
            self.columns_db = [row[1].lower() for row in data if row[1]]

        except Exception as e:
            print(f"Warning: Could not load space keywords: {e}")
            self.keywords = []
            self.columns_db = []

        # ✅ EXTENDED SPACE SYNONYMS
        self.synonyms = {
            # Satellite & Mission
            "sat": "satellite",
            "satid": "satellite",
            "sc": "spacecraft",
            "craft": "spacecraft",
            "veh": "vehicle",
            "lv": "launch_vehicle",

            # Orbit
            "orb": "orbit",
            "leo": "low_earth_orbit",
            "meo": "medium_earth_orbit",
            "geo": "geostationary_orbit",
            "heo": "high_earth_orbit",
            "alt": "altitude",
            "apo": "apogee",
            "peri": "perigee",
            "incl": "inclination",
            "ecc": "eccentricity",

            # Launch
            "lch": "launch",
            "liftoff": "launch",
            "launchdt": "launch_date",
            "pad": "launch_pad",

            # Payload
            "pl": "payload",
            "plwt": "payload_weight",
            "mass": "payload_weight",

            # Power
            "pwr": "power",
            "bat": "battery",
            "solar": "solar_panel",
            "eps": "power_system",

            # Telemetry & Communication
            "tele": "telemetry",
            "tm": "telemetry",
            "tc": "telecommand",
            "rf": "communication",
            "uplink": "communication",
            "downlink": "communication",

            # Control & Navigation
            "cmd": "command",
            "ctrl": "control",
            "gnc": "guidance_navigation_control",
            "nav": "navigation",
            "acs": "attitude_control",
            "att": "attitude",

            # Mission status
            "stat": "status",
            "health": "mission_health",
            "anom": "anomaly",
            "fail": "failure",

            # Crew / Station
            "eva": "spacewalk",
            "crew": "astronaut",
            "iss": "space_station",
            "dock": "docking",

            # Time
            "dur": "duration",
            "elapsed": "mission_duration"
        }

    def normalize(self, text):
        return str(text).lower().replace(" ", "").replace("_", "")

    # 🔍 SPACE value intelligence
    def value_pattern_score(self, series, column_name):
        score = 0
        col = series.dropna()

        if col.empty:
            return 0

        name = column_name.lower()

        # Numeric measurements
        if any(k in name for k in [
            "altitude", "apogee", "perigee",
            "payload", "power", "mass"
        ]):
            if pd.to_numeric(col, errors="coerce").notna().mean() > 0.8:
                score += 1

        # Date fields
        if "date" in name or "launch" in name:
            if pd.to_datetime(col, errors="coerce").notna().mean() > 0.7:
                score += 1

        # Status fields
        if "status" in name:
            if col.astype(str).nunique() < 10:
                score += 1

        return score

    def predict(self, csv_path):
        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            return {"error": str(e)}

        total_cols = len(df.columns)
        if total_cols == 0:
            return {"domain": "Unknown", "confidence": 0}

        matched_score = 0
        matched_keywords = []
        matched_columns = []
        match_map = []
        details = []

        for col in df.columns:
            norm_col = self.normalize(col)
            best_score = 0
            best_keyword = None
            match_type = "none"

            # 1️⃣ Fuzzy keyword + column match
            for ref in self.keywords + self.columns_db:
                s = fuzz.ratio(norm_col, self.normalize(ref))
                if s > best_score:
                    best_score = s
                    best_keyword = ref
                    match_type = "fuzzy"

            # 2️⃣ Synonym fallback
            if best_score < 85:
                for syn, actual in self.synonyms.items():
                    if syn in norm_col:
                        s = fuzz.ratio(actual, norm_col)
                        if s > best_score:
                            best_score = s
                            best_keyword = actual
                            match_type = "synonym"

            # 3️⃣ Name score
            if best_score >= 90:
                matched_score += 1
            elif best_score >= 75:
                matched_score += 0.5

            # 4️⃣ Value intelligence
            value_score = self.value_pattern_score(df[col], col)
            matched_score += value_score

            if best_score >= 75:
                matched_keywords.append(best_keyword)
                matched_columns.append(col)
                match_map.append({
                    "user_column": col,
                    "matched_keyword": best_keyword,
                    "name_score": best_score,
                    "value_score": value_score,
                    "match_type": match_type
                })

            details.append({
                "column": col,
                "name_score": best_score,
                "value_score": value_score,
                "match_type": match_type
            })

        # 5️⃣ Empty column penalty
        empty_cols = df.columns[df.isna().all()].tolist()
        matched_score -= len(empty_cols) * 0.2
        matched_score = max(0, matched_score)

        # 6️⃣ Confidence calculation
        max_possible = total_cols * 2
        confidence_100 = round((matched_score / max_possible) * 100, 2)
        confidence_10 = round(confidence_100 / 10, 2)

        # 7️⃣ Final decision
        if confidence_100 >= 85:
            decision = "CONFIRMED_SPACE"
            qualitative = "Very Strong"
        elif confidence_100 >= 65:
            decision = "LIKELY_SPACE"
            qualitative = "Strong"
        else:
            decision = "UNKNOWN"
            qualitative = "Weak"

        return {
            "domain": self.domain if decision != "UNKNOWN" else "Unknown",
            "confidence_percentage": confidence_100,
            "confidence_out_of_10": confidence_10,
            "decision": decision,
            "qualitative": qualitative,
            "total_columns": total_cols,
            "empty_columns": empty_cols,
            "matched_keywords": list(set(matched_keywords)),
            "matched_columns": matched_columns,
            "keyword_column_mapping": match_map,
            "details": details
        }
