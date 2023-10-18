from langchain.document_loaders.sitemap import SitemapLoader
from langchain.schema import Document
from typing import Any, Callable, Generator, Iterable, List, Optional, Iterator
import requests
from lxml.html import fromstring
import json


def _batch_block(iterable: Iterable, size: int) -> Generator[List[dict], None, None]:
    it = iter(iterable)
    while item := list(itertools.islice(it, size)):
        yield item

class KBSitemapLoaderExtended(SitemapLoader):
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
    
    def lazy_load(self) -> Iterator[Document]:
        
        soup = self._scrape(self.web_path, parser="xml")
        els = self.parse_sitemap(soup)

        if self.blocksize is not None:
            elblocks = list(_batch_block(els, self.blocksize))
            blockcount = len(elblocks)
            if blockcount - 1 < self.blocknum:
                raise ValueError(
                    "Selected sitemap does not contain enough blocks for given blocknum"
                )
            else:
                els = elblocks[self.blocknum]
        
        urls = [el["loc"].strip() for el in els if "loc" in el]
        urls = [url.replace("/kb", "/api/now/sp/page", 1) for url in urls] #gets kbcontent from this api

        headers = {
            "X-portal":"45d6680fdb52220099f93691f0b8f5ad",
            "Accept":"application/json"
        }

        for i in range(len(urls)):
            result = requests.get(urls[i], headers=headers)
            result = result.json()

            title = list(findkeys(result, "page_title"))
            content = list(findkeys(result, "kbContentData"))  #KB content

            if content == []:
                continue
            
            #adding title in the metadata
            metadata = self.meta_function(els[i], result)
            metadata["title"] = title[0].strip()

            #removing html tags
            parserObj = fromstring(content[0]["data"])
            outputString = str(parserObj.text_content())

            yield Document(
                page_content=outputString,
                metadata=metadata,
            )

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

def findkeys(node, kv):
    if isinstance(node, list):
        for i in node:
            for x in findkeys(i, kv):
               yield x
    elif isinstance(node, dict):
        if kv in node:
            yield node[kv]
        for j in node.values():
            for x in findkeys(j, kv):
                yield x
