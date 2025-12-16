import requests
from bs4 import BeautifulSoup
import json
import re
import time
import random
from dataclasses import dataclass, asdict
from typing import List, Optional
from datetime import datetime


# =========================
# Data Model
# =========================
@dataclass
class ReactionData:
    reaction_smiles: str
    reactant_smiles: List[str]
    reagent_smiles: List[str]
    product_smiles: List[str]
    source_url: str
    scraped_at: str


# =========================
# Scraper Class
# =========================
class KMTScraper:
    BASE_URL = "https://kmt.vander-lingen.nl"

    def __init__(self, doi: str = "10.1021/jacsau.4c01276"):
        self.doi = doi
        self.session = self._init_session()
        self.collected_reactions: List[ReactionData] = []

    def _init_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        })
        return session

    def _build_url(self, start: int = 0) -> str:
        return f"{self.BASE_URL}/data/reaction/doi/{self.doi}/start/{start}"

    def _fetch_page(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"[ERROR] Failed to fetch {url}: {e}")
            return None

    # =========================
    # SMILES Parsing
    # =========================
    def _parse_smiles_string(self, smiles: str) -> Optional[dict]:
        if not smiles:
            return None

        parts = smiles.split(">")
        if len(parts) < 3:
            parts = smiles.split(">>")
            if len(parts) == 2:
                parts = [parts[0], "", parts[1]]
            else:
                return None

        return {
            "reactants": [s for s in parts[0].split(".") if s],
            "reagents": [s for s in parts[1].split(".") if s],
            "products": [s for s in parts[2].split(".") if s],
        }

    # =========================
    # Extraction Methods
    # =========================
    def _extract_from_data_attributes(self, soup: BeautifulSoup) -> List[str]:
        return [
            el.get("data-reaction-smiles").strip()
            for el in soup.find_all(attrs={"data-reaction-smiles": True})
            if el.get("data-reaction-smiles")
        ]

    def _extract_from_javascript(self, html: str) -> List[str]:
        patterns = [
            r"reactions\.push\(\s*['\"]([^'\"]+)['\"]\s*\)",
            r"reaction[Ss]miles\s*[=:]\s*['\"]([^'\"]+)['\"]",
            r"smiles\s*:\s*['\"]([^'\"]+>>?[^'\"]+)['\"]",
        ]

        results = []
        for pattern in patterns:
            matches = re.findall(pattern, html)
            results.extend([m for m in matches if ">" in m])
        return results

    def _extract_from_tables(self, soup: BeautifulSoup) -> List[str]:
        smiles_list = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                for cell in row.find_all(["td", "th"]):
                    text = cell.get_text(strip=True)
                    if ">" in text and "." in text:
                        if re.match(r'^[A-Za-z0-9\[\]()=#@+\-\\/.>]+$', text):
                            smiles_list.append(text)
        return smiles_list

    # =========================
    # Pagination
    # =========================
    def _find_next_page_url(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        for link in soup.find_all("a", href=True):
            if "next" in link.get_text(strip=True).lower():
                href = link["href"]
                return href if href.startswith("http") else f"{self.BASE_URL}{href}"

        match = re.search(r"/start/(\d+)", current_url)
        if match:
            return self._build_url(int(match.group(1)) + 10)

        return None

    # =========================
    # Page Processing
    # =========================
    def _process_page(self, html: str, url: str) -> List[ReactionData]:
        soup = BeautifulSoup(html, "html.parser")
        all_smiles = set()

        all_smiles.update(self._extract_from_data_attributes(soup))
        all_smiles.update(self._extract_from_javascript(html))
        all_smiles.update(self._extract_from_tables(soup))

        timestamp = datetime.now().isoformat()
        reactions = []

        for smiles in all_smiles:
            parsed = self._parse_smiles_string(smiles)
            if parsed and parsed["products"]:
                reactions.append(
                    ReactionData(
                        reaction_smiles=smiles,
                        reactant_smiles=parsed["reactants"],
                        reagent_smiles=parsed["reagents"],
                        product_smiles=parsed["products"],
                        source_url=url,
                        scraped_at=timestamp,
                    )
                )

        return reactions

    # =========================
    # Main Scraping Logic
    # =========================
    def scrape(self, max_pages: int = 20, delay_range=(0.5, 1.5)) -> List[ReactionData]:
        current_url = self._build_url()
        seen_urls = set()
        pages_scraped = 0

        while current_url and pages_scraped < max_pages:
            if current_url in seen_urls:
                break

            seen_urls.add(current_url)
            print(f"Scraping: {current_url}")

            html = self._fetch_page(current_url)
            if not html:
                break

            reactions = self._process_page(html, current_url)
            for r in reactions:
                if r.reaction_smiles not in {x.reaction_smiles for x in self.collected_reactions}:
                    self.collected_reactions.append(r)

            soup = BeautifulSoup(html, "html.parser")
            current_url = self._find_next_page_url(soup, current_url)
            pages_scraped += 1

            time.sleep(random.uniform(*delay_range))

        print(f"Scraping finished. Total reactions: {len(self.collected_reactions)}")
        return self.collected_reactions

    # =========================
    # Output Helpers
    # =========================
    def to_json(self, filepath: Optional[str] = None) -> str:
        data = {
            "metadata": {
                "doi": self.doi,
                "total_reactions": len(self.collected_reactions),
                "scraped_at": datetime.now().isoformat(),
            },
            "reactions": [asdict(r) for r in self.collected_reactions],
        }

        json_data = json.dumps(data, indent=2)
        if filepath:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(json_data)
        return json_data

    def get_summary(self) -> dict:
        return {
            "total_reactions": len(self.collected_reactions),
            "doi": self.doi,
        }


# =========================
# Run Script
# =========================
if __name__ == "__main__":
    scraper = KMTScraper()
    scraper.scrape(max_pages=10)
    scraper.to_json("kmt_reactions.json")
