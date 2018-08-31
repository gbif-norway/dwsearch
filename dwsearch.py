#!/usr/bin/python
# encoding: utf-8

import os
import re
import sys
import json
import time
import yaml
import string
import urllib
import logging

from beaker.middleware import SessionMiddleware

import bottle
import bottle.ext.sqlite

from bottle import get, post, route, hook, request, redirect, run, view, abort
from bottle import static_file, template, SimpleTemplate, FormsDict

from bottle_utils.i18n import I18NPlugin, i18n_path, i18n_url, lazy_gettext as _
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, A


# basic setup
config = yaml.load(open('config.yaml'))

SESSION = {
    'session.type': 'cookie',
    'session.cookie_expires': 60 * 60 * 24 * 365,
    'session.encrypt_key': config['cookiekey'],
    'session.validate_key': True,
}
bottle.BaseRequest.MEMFILE_MAX = 1024 * 1024 * 16

es = Elasticsearch(config['hosts'], timeout=config['timeout'])

lang = config['languages'][0][0]

app = bottle.default_app()
app = I18NPlugin(app, config['languages'], config['languages'][0][0], 'lang')
app = SessionMiddleware(app, SESSION)

logging.basicConfig(level=logging.INFO)

def modifyquery(query, changes):
    query.update(changes)
    return urllib.urlencode(query)

SimpleTemplate.defaults["request"] = request
SimpleTemplate.defaults["config"] = config
SimpleTemplate.defaults["lang"] = lang
SimpleTemplate.defaults["modqry"] = modifyquery

# generates search forms based on term lists
class Form:
    def __init__(self, name, terms, request):
        self.name = name
        self.terms = terms
        self.request = request
        self.query = FormsDict.decode(request.query)

    def html(self):
        s = "<fieldset>"
        s += "<legend>%s</legend>" % _(self.name)
        for term in self.terms:
            if term['type'] == 'text':
                s += "<p>"
                s += "<label>%s</label> " % _(term['name'])
                value = self.query.get(term['name'], "")
                s += "<input value='%s' name=%s>" % (value, term['name'])
        s += "</fieldset>"
        return s

SimpleTemplate.defaults["Form"] = Form

# elasticsearch query generator
class Query:
    def __init__(self, uuid=None, core=None):
        self.uuid = uuid
        self.core = core

    def count(self, core):
        query = "_core:%s" % core
        if self.uuid:
            query += " AND _dataset:%s" % self.uuid
        if self.core:
            query += " AND _core:%s" % self.core
        re = es.count(config['index']['resolver'], q=query)
        return re['count']

    def search(self, query):
        skip = int(query.pop("skip", 0))
        s = Search(using=es, index=config['index']['resolver'])
        if self.uuid:
            s = s.query('match', _dataset=self.uuid)
        if self.core:
            s = s.query('match', _core=self.core)
        for k,v in query.iteritems():
            options = config['search'].get(k, {})
            if not v or str(v) == "":
                continue
            if ".kw" in k or options.get('type') == "keyword":
                s = s.filter('term', **{ k.replace(".term", ""): v })
            elif ".prefix" in k or options.get('type') == "prefix":
                s = s.query('prefix', **{ k.replace(".prefix", ""): v.lower() })
            elif ".fuzzy" in k or options.get('type') == "fuzzy":
                s = s.query('fuzzy', **{ k.replace(".fuzzy", ""): v.lower() })
            elif ".term" in k or options.get("type") == "term":
                s = s.query('term', **{ k.replace(".term", ""): v.lower() })
            else:
                s = s.query('match', **{ k: v })

        a = A('geo_bounds', field='_location')
        s.aggs.bucket('viewport', a)
        s = s.sort('_id')
        s = s[skip:skip + 50]
        result = s.execute()
        query['skip'] = skip
        return result

# routing etc...
# shows all datasets
@get('/')
@view('index')
def index():
    result = es.search(config['index']['datasets'], size=500)
    datasets = [d['_source'] for d in result['hits']['hits']]
    return { 'datasets': datasets }

# static files...
@route('/static/<path:path>')
def static(path):
    return static_file(path, root='static')

@get('/search')
def nocore():
    redirect("/")

# searches -all- datasets
@get('/search/<core>')
@view('search')
def search(core):
    dataset = Query()
    query = Query(core = core)
    results = query.search(request.query)
    core = config['cores'][core]
    return { 'uuid': 'search', 'meta': {}, 'dataset': dataset, 'query': query, 'results': results, 'core': core }

# shows a dataset page
@get('/<uuid>')
@view('dataset')
def dataset(uuid):
    try:
        meta = es.get(index="datasets", doc_type="meta", id=uuid).get('_source')
        dataset = Query(uuid = uuid)
        return { 'uuid': uuid, 'meta': meta, 'dataset': dataset }
    except Exception:
        abort(404, "Dataset not found")

# search a specific dataset
@get('/<uuid>/<core>')
@view('search')
def searchdataset(uuid, core):
    try:
        meta = es.get(index="datasets", doc_type="meta", id=uuid).get('_source')
        dataset = Query(uuid = uuid)
        query = Query(uuid = uuid, core = core)
        results = query.search(request.query)
        core = config['cores'][core]
        return { 'uuid': uuid, 'meta': meta, 'dataset': dataset, 'results': results, 'core': core }
    except Exception:
        abort(404, "Dataset or core not found")

if __name__ == '__main__':
    run(app, server='gunicorn', host='0.0.0.0', port=8080)

