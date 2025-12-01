# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html
import re
import json
import datetime
from typing import Any, Dict

import scrapy
from scrapy import Field
from scrapy.loader import ItemLoader

from itemloaders.processors import Identity, Compose, MapCompose, TakeFirst, Join

from dateutil.parser import parse as dateutil_parse
from w3lib.html import remove_tags


DEBUG = False


def print_schema(d, tabs=1):
    if type(d) != dict:
        return
    for key in d.keys():
        print('\t'*tabs + key)
        print_schema(d[key], tabs+1)


def visit_path(data: Dict[str, Any], key: str, original_key: str):
    if DEBUG:
        print(f"Processing {key} for {data.keys() if type(data) == dict else data}")

    if not data:
        if key and DEBUG:
            print(f'No data found for key {original_key} in data')
            print(data)
        return None

    # if no key is left, then yield the data at this point
    if not key:
        yield data
        # stop the generator since there is no more key left to parse
        return None

    if '.' in key:
        idx = key.index('.')
        subkey, remaining_key = key[:idx], key[idx + 1:]
    else:
        subkey, remaining_key = key, None

    # handle partial matches on the key
    # this is needed when the key can be dynamic
    if subkey.endswith('*'):
        # remove '*'
        subkey_prefix = subkey[:-1]

        # find all keys which match subkey_prefix
        matching_subkeys = [k for k in data.keys() if k.startswith(subkey_prefix)]

        for sk in matching_subkeys:
            yield from visit_path(data[sk], remaining_key, original_key)

        return None

    # handle arrays
    if subkey.endswith('[]'):
        # remove '[]'
        subkey = subkey[:-2]

        values = data.get(subkey, [])

        for value in values:
            yield from visit_path(value, remaining_key, original_key)

        return None

    # handle multiple comma-separated keys
    # this must be the leaf, because it doesn't make sense to extract more fields
    # from differently keyed values (at least for now)
    if subkey.startswith('[') and subkey.endswith(']'):
        subkeys = subkey[1:-1].split(",")
        value = {}
        for sk in subkeys:
            value[sk] = data.get(sk, None)
        yield value

        return None

    # handle regular keys
    yield from visit_path(data.get(subkey, None), remaining_key, original_key)

    return None



def json_field_extractor_v2(key: str):
    def extract_field(text: str):
        data = json.loads(text)
        return list(visit_path(data, key, key))
    return extract_field


def splitter(split_on=','):
    return lambda s: s.split(split_on)


def safe_parse_date(date):
    try:
        date = dateutil_parse(date, fuzzy=True, default=datetime.datetime.min)
        date = date.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        date = None

    return date


def filter_empty(vals):
    return [v.strip() for v in vals if v.strip()]


def split_by_newline(txt):
    return txt.split("\n")


def deduplicate_text(text):
    """Remove duplicated consecutive text in author bios"""
    if not text or len(text) < 100:
        return text
    
    # Try different split points to find where duplication starts
    # The duplicate typically starts around 40-60% through the text
    for split_point in range(len(text) // 3, (len(text) * 2) // 3):
        first_part = text[:split_point]
        second_part = text[split_point:]
        
        # Check if first_part appears at the start of second_part
        # Use first 100 chars as a signature to detect duplication
        signature_len = min(100, len(first_part))
        if signature_len > 50 and second_part.startswith(first_part[:signature_len]):
            return first_part
    
    return text


def extract_contributors_with_roles(key: str):
    """Extract contributor names with their roles from primary and secondary contributor edges"""
    def extract(text: str):
        data = json.loads(text)
        contributors = []
        
        # Get all Book objects
        apollo_state = data.get('props', {}).get('pageProps', {}).get('apolloState', {})
        for book_key in [k for k in apollo_state.keys() if k.startswith('Book:')]:
            book = apollo_state[book_key]
            
            # Primary contributor (usually the main author)
            primary = book.get('primaryContributorEdge')
            if primary:
                contributor_ref = primary.get('node', {}).get('__ref')
                if contributor_ref and contributor_ref in apollo_state:
                    contributor_name = apollo_state[contributor_ref].get('name')
                    if contributor_name:
                        contributors.append({
                            'name': contributor_name,
                            'role': primary.get('role', 'Unknown')
                        })
            
            # Secondary contributors (translators, editors, etc.)
            secondary = book.get('secondaryContributorEdges', [])
            for edge in secondary:
                contributor_ref = edge.get('node', {}).get('__ref')
                if contributor_ref and contributor_ref in apollo_state:
                    contributor_name = apollo_state[contributor_ref].get('name')
                    if contributor_name:
                        contributors.append({
                            'name': contributor_name,
                            'role': edge.get('role', 'Unknown')
                        })
        
        return [contributors] if contributors else []
    return extract


class BookItem(scrapy.Item):
    # Scalars
    url = Field()

    title = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.title')))
    titleComplete = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.titleComplete')))
    description = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.description'), remove_tags))
    imageUrl = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.imageUrl')))
    genres = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.bookGenres[].genre.name')), output_processor=Compose(set, list))
    asin = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.details.asin')))
    isbn = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.details.isbn')))
    isbn13 = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.details.isbn13')))
    publisher = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.details.publisher')))
    publishDate = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.details.publicationTime')))
    series = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Series*.title')), output_processor=Compose(set, list))

    author = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Contributor*.name')), output_processor=Compose(set, list))
    contributors = Field(input_processor=MapCompose(extract_contributors_with_roles('contributors')), output_processor=Compose(TakeFirst(), list))

    places = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Work*.details.places[].name')), output_processor=Compose(set, list))
    characters = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Work*.details.characters[].name')), output_processor=Compose(set, list))
    awards = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Work*.details.awardsWon[].[name,awardedAt,category,hasWon]')), output_processor=Identity())

    ratingsCount = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Work*.stats.ratingsCount')))
    reviewsCount = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Work*.stats.textReviewsCount')))
    avgRating = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Work*.stats.averageRating')))
    ratingHistogram = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Work*.stats.ratingsCountDist')))

    numPages = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.details.numPages')))
    language = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.details.language.name')))
    format = Field(input_processor=MapCompose(json_field_extractor_v2('props.pageProps.apolloState.Book*.details.language.format')))


class BookLoader(ItemLoader):
    default_output_processor = TakeFirst()


class AuthorItem(scrapy.Item):
    # Scalars
    url = Field()

    name = Field()
    birthDate = Field(input_processor=MapCompose(safe_parse_date))
    deathDate = Field(input_processor=MapCompose(safe_parse_date))

    avgRating = Field(serializer=float)
    ratingsCount = Field(serializer=int)
    reviewsCount = Field(serializer=int)

    # Lists
    genres = Field(output_processor=Compose(set, list))
    influences = Field(output_processor=Compose(set, list))

    # Blobs
    about = Field(
        # Take the first match, remove HTML tags, convert to list of lines, remove empty lines, remove the "edit data" prefix, then deduplicate
        input_processor=Compose(TakeFirst(), remove_tags, split_by_newline,
                                filter_empty, lambda s: s[1:]),
        output_processor=Compose(Join(), deduplicate_text))


class AuthorLoader(ItemLoader):
    default_output_processor = TakeFirst()
