import asyncio
import json
import traceback
from dataclasses import dataclass

import httpx
from apify import Actor
from yarl import URL

from crawlee import ConcurrencySettings, Request
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.http_clients import HttpxHttpClient


@dataclass
class ActorInput:
    """Actor input schema."""

    identifier: str
    app_password: str
    queries: list[str]
    mode: str
    max_requests_per_crawl: int | None = None


class BlueskyApiScraper:
    """A crawler class for extracting data from Bluesky social network using their official API.

    This crawler manages authentication, concurrent requests, and data collection for both
    posts and user profiles. It uses separate datasets for storing post and user information.
    """

    def __init__(self, mode: str, max_request: int | None) -> None:
        self._crawler: HttpCrawler | None = None

        self.mode = mode
        self.max_request = max_request

        # Variables for storing session data
        self._service_edpoint: str | None = None
        self._user_did: str | None = None
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._handle: str | None = None

    def create_session(self, identifier: str, password: str) -> None:
        """Create credentials for the session."""
        url = 'https://bsky.social/xrpc/com.atproto.server.createSession'
        headers = {
            'Content-Type': 'application/json',
        }
        data = {'identifier': identifier, 'password': password}

        response = httpx.post(url, headers=headers, json=data)
        response.raise_for_status()

        data = response.json()

        self._service_edpoint = data['didDoc']['service'][0]['serviceEndpoint']
        self._user_did = data['didDoc']['id']
        self._access_token = data['accessJwt']
        self._refresh_token = data['refreshJwt']
        self._handle = data['handle']

    def delete_session(self) -> None:
        """Delete the current session."""
        url = f'{self._service_edpoint}/xrpc/com.atproto.server.deleteSession'
        headers = {'Content-Type': 'application/json', 'authorization': f'Bearer {self._refresh_token}'}

        response = httpx.post(url, headers=headers)
        response.raise_for_status()

    async def init_crawler(self) -> None:
        """Initialize the crawler."""
        if not self._user_did:
            raise ValueError('Session not created.')

        # Initialize the crawler
        self._crawler = HttpCrawler(
            max_requests_per_crawl=self.max_request,
            http_client=HttpxHttpClient(
                # Set headers for API requests
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {self._access_token}',
                    'Connection': 'Keep-Alive',
                    'accept-encoding': 'gzip, deflate, br, zstd',
                }
            ),
            # Configuring concurrency of crawling requests
            concurrency_settings=ConcurrencySettings(
                min_concurrency=10,
                desired_concurrency=10,
                max_concurrency=30,
                max_tasks_per_minute=200,
            ),
        )

        self._crawler.router.default_handler(self._search_handler)  # Handler for search requests
        self._crawler.router.handler(label='user')(self._user_handler)  # Handler for user requests

    async def _search_handler(self, context: HttpCrawlingContext) -> None:
        """Handle search requests based on mode."""
        context.log.info(f'Processing search {context.request.url} ...')

        data = json.loads(context.http_response.read())

        if 'posts' not in data:
            context.log.warning(f'No posts found in response: {context.request.url}')
            return

        user_requests = {}
        posts = []

        prfile_url = URL(f'{self._service_edpoint}/xrpc/app.bsky.actor.getProfile')

        for post in data['posts']:
            # Add user request if not already added in current context
            if self.mode == 'users' and post['author']['did'] not in user_requests:
                user_requests[post['author']['did']] = Request.from_url(
                    url=str(prfile_url.with_query(actor=post['author']['did'])),
                    user_data={'label': 'user'},
                )
            elif self.mode == 'posts':
                posts.append(
                    {
                        'uri': post['uri'],
                        'cid': post['cid'],
                        'author_did': post['author']['did'],
                        'created': post['record']['createdAt'],
                        'indexed': post['indexedAt'],
                        'reply_count': post['replyCount'],
                        'repost_count': post['repostCount'],
                        'like_count': post['likeCount'],
                        'quote_count': post['quoteCount'],
                        'text': post['record']['text'],
                        'langs': '; '.join(post['record'].get('langs', [])),
                        'reply_parent': post['record'].get('reply', {}).get('parent', {}).get('uri'),
                        'reply_root': post['record'].get('reply', {}).get('root', {}).get('uri'),
                    }
                )

        if self.mode == 'posts':
            await context.push_data(posts)  # Push a batch of posts to the dataset
        else:
            await context.add_requests(list(user_requests.values()))

        if cursor := data.get('cursor'):
            next_url = URL(context.request.url).update_query({'cursor': cursor})  # Use yarl for update the query string
            await context.add_requests([str(next_url)])

    async def _user_handler(self, context: HttpCrawlingContext) -> None:
        """Handle user profile requests."""
        context.log.info(f'Processing user {context.request.url} ...')

        data = json.loads(context.http_response.read())

        user_item = {
            'did': data['did'],
            'created': data['createdAt'],
            'avatar': data.get('avatar'),
            'description': data.get('description'),
            'display_name': data.get('displayName'),
            'handle': data['handle'],
            'indexed': data.get('indexedAt'),
            'posts_count': data['postsCount'],
            'followers_count': data['followersCount'],
            'follows_count': data['followsCount'],
        }

        await context.push_data(user_item)

    async def crawl(self, queries: list[str]) -> None:
        """Crawl the given URL."""
        if not self._crawler:
            raise ValueError('Crawler not initialized.')

        search_url = URL(f'{self._service_edpoint}/xrpc/app.bsky.feed.searchPosts')

        await self._crawler.run([str(search_url.with_query(q=query)) for query in queries])


async def run() -> None:
    """Main execution function that orchestrates the crawling process.

    Creates a crawler instance, manages the session, and handles the complete
    crawling lifecycle including proper cleanup on completion or error.
    """
    async with Actor:
        raw_input = await Actor.get_input()
        actor_input = ActorInput(
            identifier=raw_input.get('indentifier', ''),
            app_password=raw_input.get('appPassword', ''),
            queries=raw_input.get('queries', []),
            mode=raw_input.get('mode', 'posts'),
            max_requests_per_crawl=raw_input.get('maxRequestsPerCrawl'),
        )
        crawler = BlueskyApiScraper(actor_input.mode, actor_input.max_requests_per_crawl)
        try:
            crawler.create_session(actor_input.identifier, actor_input.app_password)

            await crawler.init_crawler()
            await crawler.crawl(actor_input.queries)
        except httpx.HTTPError as e:
            Actor.log.error(f'HTTP error occurred: {e}')
            raise
        except Exception as e:
            Actor.log.error(f'Unexpected error: {e}')
            traceback.print_exc()
        finally:
            crawler.delete_session()


def main() -> None:
    """Entry point for the crawler application."""
    asyncio.run(run())
