"""
Business theme taxonomy for Europe–Africa gap analysis.

Themes are used for both keyword-based matching and as context for LLM
classification of think-tank publications.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


Direction = Literal["europe_to_africa", "africa_to_europe", "bilateral"]


@dataclass(frozen=True)
class Theme:
    id: str
    label: str
    direction: Direction
    description: str
    keywords: list[str]


# ── Europe → Africa ───────────────────────────────────────────────────────────

THEMES: list[Theme] = [

    Theme(
        id="fdi_investment_climate",
        label="FDI & Investment Climate",
        direction="europe_to_africa",
        description=(
            "Covers foreign direct investment flows from Europe into Africa, "
            "the investment climate and regulatory environment for European companies, "
            "and barriers and enablers to market entry on the continent."
        ),
        keywords=[
            "foreign direct investment", "FDI", "investment climate", "market entry",
            "European companies in Africa", "investor confidence", "investment barriers",
            "business environment", "regulatory environment", "investment promotion",
            "greenfield investment", "investment facilitation", "investment treaty",
            "bilateral investment treaty", "BIT", "investment protection",
        ],
    ),

    Theme(
        id="development_finance",
        label="Development Finance",
        direction="europe_to_africa",
        description=(
            "Covers development finance institutions (DFIs), blended finance structures, "
            "and concessional lending instruments deployed by European public and "
            "quasi-public financiers to mobilise private investment in Africa."
        ),
        keywords=[
            "development finance", "DFI", "development finance institution",
            "blended finance", "concessional finance", "concessional lending",
            "Invest International", "BII", "British International Investment",
            "Proparco", "DEG", "FMO", "OFID", "EBRD", "EIB", "European Investment Bank",
            "ODA", "official development assistance", "impact investing",
            "first-loss tranche", "guarantee", "subordinated debt",
            "mobilising private finance", "crowding in", "Global Gateway",
        ],
    ),

    Theme(
        id="european_business_operations",
        label="European Business Operations in Africa",
        direction="europe_to_africa",
        description=(
            "Covers the operational experience of European companies on the ground in "
            "Africa, including joint ventures, local partnerships, supply chain "
            "localisation, and corporate strategies for the African market."
        ),
        keywords=[
            "European companies in Africa", "European firms", "joint venture",
            "local partnership", "local content", "market localisation",
            "corporate strategy Africa", "doing business in Africa",
            "subsidiary", "African operations", "business presence",
            "technology transfer", "capacity building", "skills transfer",
        ],
    ),

    # ── Africa → Europe ───────────────────────────────────────────────────────

    Theme(
        id="african_market_access_europe",
        label="African Market Access to Europe",
        direction="africa_to_europe",
        description=(
            "Covers the ability of African companies and exporters to access European "
            "markets, including tariff and non-tariff barriers, export promotion, "
            "and the competitive position of African goods and services in Europe."
        ),
        keywords=[
            "African companies in Europe", "African exporters", "export to Europe",
            "market access", "market entry barriers", "tariff barriers",
            "non-tariff barriers", "NTBs", "export diversification",
            "African exports", "trade facilitation", "customs procedures",
            "rules of origin", "African products", "African services",
        ],
    ),

    Theme(
        id="eu_regulatory_barriers",
        label="EU Regulatory Barriers & Compliance",
        direction="africa_to_europe",
        description=(
            "Covers EU regulations that create compliance costs or market access "
            "barriers for African producers and exporters, including the Carbon Border "
            "Adjustment Mechanism, deforestation regulation, supply chain due diligence, "
            "and sustainability reporting requirements."
        ),
        keywords=[
            "CBAM", "carbon border adjustment mechanism", "carbon border tax",
            "EUDR", "EU deforestation regulation", "deforestation-free",
            "CSRD", "corporate sustainability reporting directive",
            "supply chain due diligence", "CSDDD", "corporate due diligence",
            "EU Green Deal", "European Green Deal", "fit for 55",
            "SPS measures", "sanitary and phytosanitary", "food safety standards",
            "EU regulations", "regulatory compliance", "trade impacts",
            "sustainability standards", "ESG requirements",
        ],
    ),

    Theme(
        id="african_capital_europe",
        label="African Capital Flows to Europe",
        direction="africa_to_europe",
        description=(
            "Covers African-origin capital deployed in European markets, including "
            "sovereign wealth funds, pension funds, diaspora remittances and investment, "
            "and African institutional investors allocating to European assets."
        ),
        keywords=[
            "African sovereign wealth fund", "SWF", "African pension fund",
            "diaspora investment", "diaspora remittances", "African capital",
            "African institutional investors", "African investors in Europe",
            "capital flows from Africa", "outward investment Africa",
            "African family offices", "African private equity Europe",
        ],
    ),

    Theme(
        id="talent_mobility",
        label="Talent Mobility & Skills Partnerships",
        direction="africa_to_europe",
        description=(
            "Covers the movement of skilled African professionals to Europe, brain drain "
            "and brain gain dynamics, talent partnership frameworks, and circular "
            "migration schemes that link European labour demand with African supply."
        ),
        keywords=[
            "talent mobility", "skills mobility", "brain drain", "brain gain",
            "African professionals in Europe", "skilled migration", "talent partnerships",
            "circular migration", "labour mobility", "migration Africa Europe",
            "talent circulation", "human capital", "diaspora professionals",
            "EU talent pool", "skills recognition", "credential recognition",
        ],
    ),

    # ── Bilateral ─────────────────────────────────────────────────────────────

    Theme(
        id="trade_agreements",
        label="Trade Agreements & Policy Frameworks",
        direction="bilateral",
        description=(
            "Covers formal trade agreements and policy frameworks shaping Europe–Africa "
            "trade, including the AfCFTA, Economic Partnership Agreements, WTO "
            "commitments, and preferential trade arrangements."
        ),
        keywords=[
            "AfCFTA", "African Continental Free Trade Area",
            "EPA", "Economic Partnership Agreement",
            "WTO", "World Trade Organization",
            "preferential trade", "trade agreement", "free trade agreement", "FTA",
            "trade policy", "trade negotiations", "market liberalisation",
            "trade cooperation", "EU-Africa trade", "trade framework",
            "most favoured nation", "MFN", "generalised system of preferences", "GSP",
            "Everything But Arms", "EBA",
        ],
    ),

    Theme(
        id="value_chains",
        label="Value Chain Integration & Industrialisation",
        direction="bilateral",
        description=(
            "Covers integration of African economies into global and regional value "
            "chains, value capture and upgrading strategies, industrialisation policy, "
            "and local content requirements that affect Europe–Africa business linkages."
        ),
        keywords=[
            "global value chains", "GVC", "regional value chains", "value chain",
            "value chain integration", "value capture", "value addition",
            "industrialisation", "industrial policy", "manufacturing",
            "local content", "backward linkages", "forward linkages",
            "economic diversification", "structural transformation",
            "upstream", "downstream", "processing", "beneficiation",
        ],
    ),

    Theme(
        id="standards_certification",
        label="Standards, Certification & Technical Barriers",
        direction="bilateral",
        description=(
            "Covers technical standards, product certification, mutual recognition "
            "agreements, and technical barriers to trade that affect the movement of "
            "goods and services between Africa and Europe."
        ),
        keywords=[
            "standards", "certification", "technical standards", "product standards",
            "mutual recognition", "mutual recognition agreement", "MRA",
            "technical barriers to trade", "TBT", "conformity assessment",
            "quality infrastructure", "accreditation", "metrology",
            "ISO", "international standards", "harmonisation",
            "regulatory convergence", "testing", "labelling requirements",
        ],
    ),

    Theme(
        id="digital_economy",
        label="Digital Economy & Cross-Border Digital Trade",
        direction="bilateral",
        description=(
            "Covers digital trade, fintech, e-commerce platforms, and the policy and "
            "regulatory frameworks governing cross-border digital services and data "
            "flows between Africa and Europe."
        ),
        keywords=[
            "digital economy", "digital trade", "e-commerce", "fintech",
            "digital platforms", "platform economy", "cross-border digital",
            "data flows", "data localisation", "digital payments",
            "mobile money", "digital financial services", "digital infrastructure",
            "internet economy", "tech startups", "digital transformation",
            "artificial intelligence", "AI", "cloud computing",
        ],
    ),

    Theme(
        id="critical_minerals",
        label="Critical Minerals & Resources",
        direction="bilateral",
        description=(
            "Covers African mineral resources strategic to European supply chains, "
            "including battery metals for the energy transition, EU critical raw "
            "materials strategy, and beneficiation and value-addition opportunities "
            "for African mineral producers."
        ),
        keywords=[
            "critical minerals", "critical raw materials", "CRM",
            "battery metals", "lithium", "cobalt", "nickel", "manganese",
            "rare earth elements", "REE", "copper", "graphite",
            "mining", "extractives", "beneficiation", "mineral processing",
            "EU Critical Raw Materials Act", "CRMA",
            "energy transition minerals", "clean energy supply chain",
            "mineral value chain", "artisanal mining", "ASGM",
        ],
    ),

    Theme(
        id="agribusiness_food_trade",
        label="Agribusiness & Food Value Chains",
        direction="bilateral",
        description=(
            "Covers agricultural trade and investment between Africa and Europe, "
            "food value chains, agribusiness investment, and the SPS and food safety "
            "standards that govern market access for African agricultural exports."
        ),
        keywords=[
            "agribusiness", "agricultural trade", "food value chain", "food trade",
            "agricultural exports", "agricultural investment",
            "SPS measures", "food safety", "phytosanitary",
            "horticulture", "cocoa", "coffee", "cashew", "cotton",
            "smallholder farmers", "food systems", "agroindustry",
            "climate-smart agriculture", "sustainable agriculture",
        ],
    ),

    # ── Cross-cutting (Finance) ───────────────────────────────────────────────

    Theme(
        id="access_to_finance",
        label="Access to Finance",
        direction="bilateral",
        description=(
            "Covers financing constraints facing African firms and entrepreneurs, "
            "including SME finance, capital markets development, cross-border capital "
            "raising, and fintech solutions that expand credit access."
        ),
        keywords=[
            "access to finance", "SME finance", "small business finance",
            "capital markets", "African capital markets", "stock exchange",
            "bond markets", "credit access", "financial inclusion",
            "microfinance", "venture capital", "private equity",
            "raising capital", "African firms raising capital",
            "lending", "credit guarantee", "collateral",
        ],
    ),

    Theme(
        id="risk_derisking",
        label="Risk & De-risking",
        direction="bilateral",
        description=(
            "Covers political risk, currency risk, and the instruments and institutions "
            "used to mitigate investment risk in African markets, including guarantees, "
            "insurance, and first-loss structures that crowd in private capital."
        ),
        keywords=[
            "political risk", "investment risk", "country risk",
            "currency risk", "exchange rate risk", "FX risk",
            "investment guarantee", "risk guarantee", "credit guarantee",
            "MIGA", "multilateral investment guarantee agency",
            "de-risking", "risk mitigation", "first-loss",
            "political risk insurance", "export credit",
            "export credit agency", "ECA", "risk sharing",
        ],
    ),

]


# ── Helpers ───────────────────────────────────────────────────────────────────

_INDEX: dict[str, Theme] = {t.id: t for t in THEMES}


def get_all_themes() -> list[Theme]:
    """Return the full list of themes."""
    return THEMES


def get_theme_by_id(theme_id: str) -> Theme:
    """Return a Theme by its id. Raises KeyError if not found."""
    return _INDEX[theme_id]
