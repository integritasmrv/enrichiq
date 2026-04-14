from .nominatim_collector import NominatimCollector
from .overpass_collector import OverpassCollector
from .base import BaseCollector, RawEvidence
from .scrapiq_collector import ScrapIQWebDiscoveryCollector
from .social_analyzer_collector import SocialAnalyzerCollector

COLLECTOR_REGISTRY = {
    "nominatim_geocode": NominatimCollector,
    "overpass_lookup": OverpassCollector,
    "scrapiq_web_discovery": ScrapIQWebDiscoveryCollector,
    "social_analyzer_api": SocialAnalyzerCollector,
}
