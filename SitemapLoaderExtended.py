from langchain.document_loaders.sitemap import SitemapLoader
from langchain.schema import Document
from typing import Any, Callable, Generator, Iterable, List, Optional, Iterator
import re
import json


def _batch_block(iterable: Iterable, size: int) -> Generator[List[dict], None, None]:
    it = iter(iterable)
    while item := list(itertools.islice(it, size)):
        yield item

class SitemapLoaderExtended(SitemapLoader):
    def __init__(
        self,
        web_path: str,
        filter_urls: Optional[List[str]] = None,
        parsing_function: Optional[Callable] = None,
        blocksize: Optional[int] = None,
        blocknum: int = 0,
        meta_function: Optional[Callable] = None,
        is_local: bool = False,
        continue_on_failure: bool = False,
        **kwargs: Any,
    ):
        super().__init__(web_path, filter_urls, parsing_function, blocksize, blocknum, meta_function, is_local, continue_on_failure, **kwargs)
        self.allow_url_patterns = filter_urls
    
    
    def lazy_load(self) -> Iterator[Document]:
        
        # soup = self._scrape(self.web_path, parser="xml")
        # els = self.parse_sitemap(soup)

        # if self.blocksize is not None:
        #     elblocks = list(_batch_block(els, self.blocksize))
        #     blockcount = len(elblocks)
        #     if blockcount - 1 < self.blocknum:
        #         raise ValueError(
        #             "Selected sitemap does not contain enough blocks for given blocknum"
        #         )
        #     else:
        #         els = elblocks[self.blocknum]
        
        # urls = [el["loc"].strip() for el in els if "loc" in el]

        # for i in range(len(urls)):
        #     result = self._scrape(urls[i])
        #     yield Document(
        #         page_content=self.parsing_function(result),
        #         metadata=self.meta_function(els[i], result),
        #     )

        for sitemap_url in self.scrape_sitemap_index():
            yield from self.scrape_page_urls(sitemap_url)
    
    def scrape_page_urls(self, sitemap_url):
        soup = self._scrape(sitemap_url, parser="xml")
        page_urls = self.parse_sitemap(soup)

        urls = [el["loc"].strip() for el in page_urls if "loc" in el]

        for i in range(len(urls)):
            result = self._scrape(urls[i])
            result = result.find_all("section")[0]
            
            title = result.find_all("div", {"class": "title"})[0]
            # content = self.parsing_function(result).split("|")
            # title = content[0]

            metadata=self.meta_function(page_urls[i], result)
            metadata["title"] = title.get_text()

            title.decompose()
            content = result.get_text()

            yield Document(
                page_content=content,
                metadata=metadata,
            )
    
    def scrape_sitemap_index(self):
        soup = self._scrape(self.web_path, parser="xml")
        sitemap_tags = soup.find_all('sitemap')
        for sitemap in sitemap_tags:  
            sitemap_url = sitemap.find('loc').string  
            if self.allow_url_patterns and not any(
                re.match(regexp_pattern, sitemap_url)
                for regexp_pattern in self.allow_url_patterns
            ):
                continue
            yield sitemap_url

    def save_docs_to_jsonl(self, array:Iterable[Document], file_path:str)->None:
        with open(file_path, 'w') as jsonl_file:
            for doc in array:
                jsonl_file.write(doc.json() + '\n')

    def load_docs_from_jsonl(self, file_path)->Iterable[Document]:
        array = []
        with open(file_path, 'r') as jsonl_file:
            for line in jsonl_file:
                data = json.loads(line)
                obj = Document(**data)
                array.append(obj)
        return array