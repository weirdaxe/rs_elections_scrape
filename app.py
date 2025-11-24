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
    # JSON first (this matches the response you showed)
    candidates = parse_json_candidates(raw_text)
    if candidates:
        return candidates

    # Fallback to XML if JSON parsing yields nothing
    return parse_xml_candidates(raw_text)


def scrape_results(start_id: int, end_id: int, webresult_key: str, progress_callback=None) -> pd.DataFrame:
    """
    Scrape polling stations from start_id to end_id (inclusive) and
    return a DataFrame: rows = polling_station_id, columns = candidate names.

    progress_callback signature:
        progress_callback(current_index, total, polling_id, station_result_dict, raw_response)
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
        tqdm(range(start_id, end_id + 1), desc="Scraping", unit="station"),
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

        # Parse (JSON or XML)
        candidates = parse_candidates(raw_response)

        station_result = {}
        for name, votes in candidates:
            station_result[name] = votes
            all_candidates.add(name)

        results_by_station[polling_id] = station_result

        if progress_callback:
            progress_callback(idx, total, polling_id, station_result, raw_response)

    # Build DataFrame
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


def main():
    st.title("Izbori Polling Station Scraper")

    st.markdown(
        """
Scrapes polling station candidate results from:

`https://www.izbori.ba/api_2018/race5_pollingstationscandidatesresult/...`
        """
    )

    webresult_key = st.text_input(
        "WEBRESULT key (leave default unless changed on the site)",
        value=DEFAULT_WEBRESULT_KEY,
    )

    col1, col2 = st.columns(2)
    start_id = col1.number_input("Start polling station ID", min_value=1, value=2, step=1)
    end_id = col2.number_input("End polling station ID", min_value=1, value=2165, step=1)

    if start_id > end_id:
        st.error("Start ID must be <= End ID")
        return

    run_button = st.button("Run scraper and generate CSV")

    if run_button:
        progress_bar = st.progress(0.0)
        status_text = st.empty()

        # Parsed results log
        st.subheader("Live scrape log (parsed)")
        log_box = st.empty()
        log_lines = []

        # Raw response log (last response only)
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
            # log_text = "\n".join(log_lines[-200:])  # keep last 200 lines
            # log_box.text(log_text)

            if raw_response:
                raw_box.text(raw_response[:4000])  # truncate if very long

        st.write("Scraping in progress...")

        df = scrape_results(int(start_id), int(end_id), webresult_key, progress_callback)

        status_text.text("Scraping finished.")
        progress_bar.progress(1.0)

        st.write(f"Rows (polling stations): {df.shape[0]}")
        st.write(f"Columns (candidates): {df.shape[1]}")

        st.dataframe(df.head())

        csv_bytes = df.to_csv(encoding="utf-8").encode("utf-8")

        st.download_button(
            label="Download CSV",
            data=csv_bytes,
            file_name="polling_station_results.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()
