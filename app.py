import streamlit as st
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import json
from tqdm import tqdm

# Default WebResult key from your example; change via UI if needed
DEFAULT_WEBRESULT_KEY = "WebResult_2022GENP1_2025_11_19_14_41_56"


def parse_json_candidates(text: str):
    """
    Try to parse candidates from JSON text.
    Expects a list of objects with at least 'name' and 'totalVotes' keys.
    """
    if not text:
        return []

    s = text.strip()
    if not s:
        return []

    try:
        if s[0] not in ("[", "{"):
            return []
        data = json.loads(s)
    except Exception:
        return []

    candidates = []

    # If it's a list of candidate objects
    if isinstance(data, list):
        iterable = data
    else:
        # If it's a dict, try common wrappers, otherwise treat as single item
        iterable = data.get("results") or data.get("Candidates") or [data]

    for item in iterable:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("Name")
        votes = item.get("totalVotes") or item.get("TotalVotes") or 0
        if not name:
            continue
        try:
            votes = int(votes)
        except (ValueError, TypeError):
            votes = 0
        candidates.append((name.strip(), votes))

    return candidates


def parse_xml_candidates(xml_text: str):
    """
    Parse one XML response and return list of (candidate_name, total_votes).
    Robust to empty or malformed XML and to namespaces.
    """
    if not xml_text:
        return []

    xml_text = xml_text.strip()
    if not xml_text:
        return []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    candidates = []

    # Namespace-aware attempt
    if root.tag.startswith("{"):
        ns_uri = root.tag.split("}")[0].strip("{")
        ns = {"ns": ns_uri}
        item_tag = "ns:Race5_PollingStationsCandidatesResult"
        name_tag = "ns:Name"
        votes_tag = "ns:TotalVotes"
    else:
        ns = {}
        item_tag = "Race5_PollingStationsCandidatesResult"
        name_tag = "Name"
        votes_tag = "TotalVotes"

    for elem in root.findall(item_tag, ns):
        name_elem = elem.find(name_tag, ns)
        votes_elem = elem.find(votes_tag, ns)

        if name_elem is None or name_elem.text is None:
            continue

        name = name_elem.text.strip()
        try:
            votes = int(votes_elem.text) if votes_elem is not None and votes_elem.text else 0
        except (ValueError, TypeError):
            votes = 0

        candidates.append((name, votes))

    # Fallback: strip namespaces and inspect children
    if not candidates:
        for elem in root:
            elem_local = elem.tag.split("}")[-1]
            if elem_local != "Race5_PollingStationsCandidatesResult":
                continue

            name_text = None
            votes_val = 0

            for child in elem:
                child_local = child.tag.split("}")[-1]

                if child_local == "Name":
                    name_text = (child.text or "").strip()
                elif child_local == "TotalVotes":
                    try:
                        votes_val = int(child.text) if child.text else 0
                    except (ValueError, TypeError):
                        votes_val = 0

            if name_text:
                candidates.append((name_text, votes_val))

    return candidates


def parse_candidates(raw_text: str):
    """
    Unified parser: try JSON first, then XML.
    Returns list of (candidate_name, total_votes).
    """
    candidates = parse_json_candidates(raw_text)
    if candidates:
        return candidates
    return parse_xml_candidates(raw_text)


def scrape_candidate_results(start_id: int, end_id: int, webresult_key: str, progress_callback=None) -> pd.DataFrame:
    """
    Scrape candidate results from start_id to end_id (inclusive).
    """
    base_url = (
        "https://www.izbori.ba/api_2018/"
        "race5_pollingstationscandidatesresult/%22" + webresult_key + "%22/{polling_id}/4"
    )

    all_candidates = set()
    results_by_station = {}
    session = requests.Session()

    total = end_id - start_id + 1

    for idx, polling_id in enumerate(
        tqdm(range(start_id, end_id + 1), desc="Scraping candidates", unit="station"),
        start=1
    ):
        url = base_url.format(polling_id=polling_id)

        station_result = {}
        raw_response = ""

        try:
            resp = session.get(url, timeout=1)
            raw_response = resp.text
        except Exception:
            results_by_station[polling_id] = station_result
            if progress_callback:
                progress_callback(idx, total, polling_id, station_result, raw_response)
            continue

        if resp.status_code != 200:
            results_by_station[polling_id] = station_result
            if progress_callback:
                progress_callback(idx, total, polling_id, station_result, raw_response)
            continue

        candidates = parse_candidates(raw_response)

        station_result = {}
        for name, votes in candidates:
            station_result[name] = votes
            all_candidates.add(name)

        results_by_station[polling_id] = station_result

        if progress_callback:
            progress_callback(idx, total, polling_id, station_result, raw_response)

    all_candidates = sorted(all_candidates)

    data = []
    index = []

    for polling_id in sorted(results_by_station.keys()):
        station_result = results_by_station[polling_id]
        row = [station_result.get(cand, 0) for cand in all_candidates]
        data.append(row)
        index.append(polling_id)

    df = pd.DataFrame(data, index=index, columns=all_candidates)
    df.index.name = "polling_station_id"

    return df


def parse_basicinfo(raw_text: str):
    """
    Parse polling station basic info (ballot statistics) from JSON or XML.
    Returns a dict of {field_name: value}.
    """
    if not raw_text:
        return {}

    s = raw_text.strip()
    if not s:
        return {}

    # Try JSON first
    try:
        if s[0] in ("[", "{"):
            data = json.loads(s)
        else:
            data = None
    except Exception:
        data = None

    if data is not None:
        obj = None

        if isinstance(data, list):
            if data and isinstance(data[0], dict):
                obj = data[0]
        elif isinstance(data, dict):
            # If dict, either already the object or wrapped
            # Simple heuristic: if any value is a dict and key looks like basic info, unwrap it
            lower_keys = {k.lower(): k for k in data.keys()}
            candidate_key = None
            for k_lower, k_orig in lower_keys.items():
                if "basicinfo" in k_lower or "race5_pollingstationsbasicinfo" in k_lower:
                    candidate_key = k_orig
                    break
            if candidate_key and isinstance(data[candidate_key], dict):
                obj = data[candidate_key]
            else:
                obj = data

        if isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                # Try to coerce numeric values
                if isinstance(v, (int, float)):
                    result[k] = v
                else:
                    try:
                        fv = float(v)
                        # cast to int if it's an integer
                        if fv.is_integer():
                            result[k] = int(fv)
                        else:
                            result[k] = fv
                    except Exception:
                        result[k] = v
            return result

    # Fallback: XML
    try:
        root = ET.fromstring(s)
    except ET.ParseError:
        return {}

    result = {}
    for child in root:
        key = child.tag.split("}")[-1]
        text = (child.text or "").strip()
        if not text:
            result[key] = 0
            continue
        try:
            fv = float(text)
            if fv.is_integer():
                result[key] = int(fv)
            else:
                result[key] = fv
        except Exception:
            result[key] = text

    return result


def scrape_basicinfo(start_id: int, end_id: int, webresult_key: str, progress_callback=None) -> pd.DataFrame:
    """
    Scrape polling station basic info (ballot statistics) from start_id to end_id (inclusive).
    """
    base_url = (
        "https://www.izbori.ba/api_2018/"
        "race5_pollingstationsbasicinfo/%22" + webresult_key + "%22/{polling_id}"
    )

    all_fields = set()
    results_by_station = {}
    session = requests.Session()

    total = end_id - start_id + 1

    for idx, polling_id in enumerate(
        tqdm(range(start_id, end_id + 1), desc="Scraping basic info", unit="station"),
        start=1
    ):
        url = base_url.format(polling_id=polling_id)

        station_result = {}
        raw_response = ""

        try:
            resp = session.get(url, timeout=1)
            raw_response = resp.text
        except Exception:
            results_by_station[polling_id] = station_result
            if progress_callback:
                progress_callback(idx, total, polling_id, station_result, raw_response)
            continue

        if resp.status_code != 200:
            results_by_station[polling_id] = station_result
            if progress_callback:
                progress_callback(idx, total, polling_id, station_result, raw_response)
            continue

        info = parse_basicinfo(raw_response)

        station_result = info
        for field in info.keys():
            all_fields.add(field)

        results_by_station[polling_id] = station_result

        if progress_callback:
            progress_callback(idx, total, polling_id, station_result, raw_response)

    all_fields = sorted(all_fields)

    data = []
    index = []

    for polling_id in sorted(results_by_station.keys()):
        station_result = results_by_station[polling_id]
        row = [station_result.get(field, 0) for field in all_fields]
        data.append(row)
        index.append(polling_id)

    df = pd.DataFrame(data, index=index, columns=all_fields)
    df.index.name = "polling_station_id"

    return df


def main():
    st.title("Izbori Polling Station Scraper")

    st.markdown(
        """
Scrapes polling station data from:

- Candidate results endpoint  
- Polling box statistics (basic info) endpoint
        """
    )

    webresult_key = st.text_input(
        "WEBRESULT key (leave default unless changed on the site)",
        value=DEFAULT_WEBRESULT_KEY,
    )

    tab_candidates, tab_basicinfo = st.tabs(["Candidate results", "Polling box stats"])

    with tab_candidates:
        st.header("Candidate results")

        col1, col2 = st.columns(2)
        start_id = col1.number_input("Start polling station ID", min_value=1, value=2, step=1, key="cand_start")
        end_id = col2.number_input("End polling station ID", min_value=1, value=2165, step=1, key="cand_end")

        if start_id > end_id:
            st.error("Start ID must be <= End ID")
        else:
            run_button = st.button("Run candidate scraper and generate CSV")

            if run_button:
                progress_bar = st.progress(0.0)
                status_text = st.empty()

                # Parsed results log
                st.subheader("Live scrape log (parsed)")
                log_box = st.empty()
                log_lines = []

                # Raw response log
                st.subheader("Last raw response (JSON/XML)")
                raw_box = st.empty()

                def progress_callback(current_index, total, current_polling_id, station_result, raw_response):
                    frac = current_index / total
                    progress_bar.progress(frac)
                    status_text.text(
                        f"Scraping {current_index}/{total} (polling station ID {current_polling_id})"
                    )

                    if station_result:
                        details = ", ".join(
                            f"{name}={votes}" for name, votes in station_result.items()
                        )
                    else:
                        details = "no candidates parsed (HTTP error, empty body, or parse failure)"

                    # log_lines.append(f"{current_index}/{total} | ID {current_polling_id}: {details}")
                    # log_text = "\n".join(log_lines[-200:])
                    # log_box.text(log_text)

                    if raw_response:
                        raw_box.text(raw_response[:4000])

                st.write("Scraping candidate results...")

                df = scrape_candidate_results(int(start_id), int(end_id), webresult_key, progress_callback)

                status_text.text("Scraping finished.")
                progress_bar.progress(1.0)

                st.write(f"Rows (polling stations): {df.shape[0]}")
                st.write(f"Columns (candidates): {df.shape[1]}")

                st.dataframe(df.head())

                csv_bytes = df.to_csv(encoding="utf-8").encode("utf-8")

                st.download_button(
                    label="Download candidate results CSV",
                    data=csv_bytes,
                    file_name="polling_station_candidate_results.csv",
                    mime="text/csv",
                )

    with tab_basicinfo:
        st.header("Polling box statistics (basic info)")

        col1, col2 = st.columns(2)
        start_id_bi = col1.number_input("Start polling station ID", min_value=1, value=2, step=1, key="bi_start")
        end_id_bi = col2.number_input("End polling station ID", min_value=1, value=2165, step=1, key="bi_end")

        if start_id_bi > end_id_bi:
            st.error("Start ID must be <= End ID")
        else:
            run_button_bi = st.button("Run basic info scraper and generate CSV")

            if run_button_bi:
                progress_bar_bi = st.progress(0.0)
                status_text_bi = st.empty()

                st.subheader("Live scrape log (parsed)")
                log_box_bi = st.empty()
                log_lines_bi = []

                st.subheader("Last raw response (JSON/XML)")
                raw_box_bi = st.empty()

                def progress_callback_bi(current_index, total, current_polling_id, station_result, raw_response):
                    frac = current_index / total
                    progress_bar_bi.progress(frac)
                    status_text_bi.text(
                        f"Scraping {current_index}/{total} (polling station ID {current_polling_id})"
                    )

                    if station_result:
                        details = ", ".join(
                            f"{k}={v}" for k, v in station_result.items()
                        )
                    else:
                        details = "no basic info parsed (HTTP error, empty body, or parse failure)"

                    # log_lines_bi.append(f"{current_index}/{total} | ID {current_polling_id}: {details}")
                    # log_text = "\n".join(log_lines_bi[-200:])
                    # log_box_bi.text(log_text)

                    if raw_response:
                        raw_box_bi.text(raw_response[:4000])

                st.write("Scraping polling box statistics...")

                df_bi = scrape_basicinfo(int(start_id_bi), int(end_id_bi), webresult_key, progress_callback_bi)

                status_text_bi.text("Scraping finished.")
                progress_bar_bi.progress(1.0)

                st.write(f"Rows (polling stations): {df_bi.shape[0]}")
                st.write(f"Columns (stats fields): {df_bi.shape[1]}")

                st.dataframe(df_bi.head())

                csv_bytes_bi = df_bi.to_csv(encoding="utf-8").encode("utf-8")

                st.download_button(
                    label="Download polling box stats CSV",
                    data=csv_bytes_bi,
                    file_name="polling_station_basicinfo_results.csv",
                    mime="text/csv",
                )


if __name__ == "__main__":
    main()
