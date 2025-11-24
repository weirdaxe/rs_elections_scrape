import streamlit as st
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from tqdm import tqdm

# Default WebResult key from your example; change via UI if needed
DEFAULT_WEBRESULT_KEY = "WebResult_2022GENP1_2025_11_19_14_41_56"


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
        # Malformed XML – treat as no data for this polling station
        return []

    candidates = []

    # --- First attempt: namespace-aware parsing (original logic) ----------------
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

    # --- Fallback: namespace-agnostic, using local tag names -------------------
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


def scrape_results(start_id: int, end_id: int, webresult_key: str, progress_callback=None) -> pd.DataFrame:
    """
    Scrape polling stations from start_id to end_id (inclusive) and
    return a DataFrame: rows = polling_station_id, columns = candidate names.

    progress_callback signature:
        progress_callback(current_index, total, polling_id, station_result_dict, raw_xml)
    """
    base_url = (
        "https://www.izbori.ba/api_2018/"
        "race5_pollingstationscandidatesresult/%22" + webresult_key + "%22/{polling_id}/4"
    )

    all_candidates = set()
    results_by_station = {}
    session = requests.Session()

    total = end_id - start_id + 1

    # tqdm for server logs (if running in terminal)
    for idx, polling_id in enumerate(
        tqdm(range(start_id, end_id + 1), desc="Scraping", unit="station"),
        start=1
    ):
        url = base_url.format(polling_id=polling_id)

        station_result = {}
        raw_xml = ""

        try:
            resp = session.get(url, timeout=10)
            raw_xml = resp.text
        except Exception:
            results_by_station[polling_id] = station_result
            if progress_callback:
                progress_callback(idx, total, polling_id, station_result, raw_xml)
            continue

        if resp.status_code != 200:
            results_by_station[polling_id] = station_result
            if progress_callback:
                progress_callback(idx, total, polling_id, station_result, raw_xml)
            continue

        # Parse XML
        candidates = parse_xml_candidates(raw_xml)

        station_result = {}
        for name, votes in candidates:
            station_result[name] = votes
            all_candidates.add(name)

        results_by_station[polling_id] = station_result

        if progress_callback:
            progress_callback(idx, total, polling_id, station_result, raw_xml)

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

        # Raw XML log (last response only)
        st.subheader("Last raw XML response")
        raw_box = st.empty()

        def progress_callback(current_index, total, current_polling_id, station_result, raw_xml):
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
                details = "no candidates parsed (HTTP error, empty XML, or parse failure)"

            log_lines.append(f"{current_index}/{total} | ID {current_polling_id}: {details}")
            log_text = "\n".join(log_lines[-200:])  # keep last 200 lines
            log_box.text(log_text)

            # Show truncated raw XML for the latest request
            if raw_xml:
                raw_box.text(raw_xml[:4000])  # truncate if very long

        st.write("Scraping in progress...")

        df = scrape_results(int(start_id), int(end_id), webresult_key, progress_callback)

        status_text.text("Scraping finished.")
        progress_bar.progress(1.0)

        st.write(f"Rows (polling stations): {df.shape[0]}")
        st.write(f"Columns (candidates): {df.shape[1]}")

        # Optional quick preview
        st.dataframe(df.head())

        # Prepare CSV bytes directly for download
        csv_bytes = df.to_csv(encoding="utf-8").encode("utf-8")

        st.download_button(
            label="Download CSV",
            data=csv_bytes,
            file_name="polling_station_results.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()
