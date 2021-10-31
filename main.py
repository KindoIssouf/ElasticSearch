import gzip
from lxml import etree
import json
from elasticsearch import Elasticsearch, helpers
from collections import deque



def gendata():
    # Your code here; this is a generator function. there's an example on one of the lecture slides
    data = gzip.open('//Users/youssefkindo/Downloads/pubmed21n0018.xml.gz', 'rb')
    tree = etree.parse(data)
    root = tree.getroot()

    data_list = {
        "PMID": [],
        "Authors": [],
        "Abstract": [],
        "ArticleTitle": [],
        "Journal": [],
        "MeshIDs": [],
        "PublishDate": [],
        "Title": [],
        "Keywords": []
    }
    # ArticleTitle

    for node in root.iter():
        if node.tag == "PubmedArticle":
            data_list["PMID"].append(node[0][0].text)
        if node.tag == "AbstractText":
            data_list['Abstract'].append(node.text)
        if node.tag == "AuthorList":
            authors = []
            for author in node:
                authors.append([name.text + " " for name in author])
            data_list['Authors'].append(authors)
        if node.tag == "Journal":
            data_list['Journal'].append(node[2].text)
        if node.tag == "DateCompleted":
            pubDate = node[0].text + "-" + node[1].text + "-" + node[2].text
            data_list['PublishDate'].append(pubDate)
        if node.tag == "ArticleTitle":
            data_list['Title'].append(node.text)
        if node.tag == "KeywordList":
            keywords = []
            for keyword in node:
                keywords.append(keyword.text)
            data_list['Keywords'].append(keywords)
        if node.tag == 'MeshHeadingList':
            MeshIDs = []
            for mesh in node:
                MeshIDs.append(mesh[0].get("UI"))
            data_list['MeshIDs'].append(MeshIDs)
    for i in range(len(root)):
        article = {
            "Uploader": "Issouf Kindo",
        }
        for key, val in data_list.items():
            if i < len(val):
                article[key] = val[i]
        yield article

if __name__ == '__main__':
    index_name = 'pubmed2021'
    Elasticsearch()
    es = Elasticsearch(hosts=['10.80.34.86:9200'], http_auth=('elastic', 'iYYX96TPlAJ000UJ0vqa'))
    deque(helpers.parallel_bulk(es, gendata(), index=index_name), maxlen=0)
    es.indices.refresh()

# curl -u "elastic:iYYX96TPlAJ000UJ0vqa" -XGET "10.80.34.86:9200/pubmed2021/_search" -H 'Content-Type: application/json' --data-binary @data.json
