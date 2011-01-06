#!/usr/bin/env python
#
# Copyright 2010 Hareesh Nagarajan.

__author__ = 'hareesh.nagarajan@gmail.com (Hareesh Nagarajan)'

import datetime
import logging
import os
import urllib
import time
import wsgiref.handlers
from django.utils import simplejson as json
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template

VERSION='v2'
CLIENT_ID='903719eb0eaf2598b2ef57be9507136fd0386b09'
DIVISIONS_URL='http://api.groupon.com/%s/divisions?client_id=%s' % (VERSION, CLIENT_ID)
__DEAL_URL='http://api.groupon.com/%s/deals?client_id=%s&division_id=' % (VERSION, CLIENT_ID)
DEAL_URL=__DEAL_URL + '%s'
DATE_PATTERN="%Y-%m-%dT%H:%M:%SZ"
SECONDS_IN_DAY=86400.0

######################################################################
##
##      EXCEPTION CLASSES
##
######################################################################
class FetchError(Exception):
  pass

######################################################################
##
##      DATABASE MODEL
##
######################################################################

class Syncs(db.Model):
  date = db.DateTimeProperty(indexed=True, auto_now_add=True)
  sync_time = db.StringProperty(indexed=True)
  revenue = db.FloatProperty()
  
class Deal(db.Model):
  sync_time = db.StringProperty(indexed=True)
  division_id = db.StringProperty(indexed=True)
  title = db.StringProperty()
  url = db.LinkProperty()
  tipped = db.BooleanProperty()
  quantity_sold = db.IntegerProperty()
  price = db.FloatProperty()
  currency = db.StringProperty(indexed=True)
  # How many days is the deal on for? The revenue is normalized for 1
  # day.
  days = db.FloatProperty()
  revenue = db.FloatProperty()

class Revenue(db.Model):
  sync_time = db.StringProperty(indexed=True)
  revenue = db.FloatProperty()

######################################################################
##
##      Helper Functions
##
######################################################################

def commaify(value):
  value = str(value)
  if value.find('.') != -1 or len(value) <= 3:
    return value
  return ''.join(commaify(value[:-3]) + ',' + value[-3:])

def FetchAndParse(url):
  result = None
  try:
    result = urlfetch.fetch(url)
  except Exception, e:
    logging.error('Could not fetch: %s (%s)', url, str(e))
    raise FetchError
  return json.loads(result.content)

def DivisionList():
  divisions=[]
  stuff=FetchAndParse(DIVISIONS_URL)
  for div in stuff['divisions']:
    divisions.append(div['id'])
  return divisions

def Process():
  time_now = datetime.datetime.now()
  sync_time = str(time_now)
  objects = []
  total_revenue = 0.0
  for division_id in DivisionList():
    logging.info('Processing ... %s', division_id)
    deals=FetchAndParse(DEAL_URL % division_id)['deals']
    for deal in deals:
      obj = Deal()
      obj.sync_time = sync_time
      obj.division_id = division_id
      obj.title = deal['title']
      obj.url = deal['dealUrl']
      obj.tipped = deal['isTipped']
      obj.quantity_sold = deal['soldQuantity']
      # Get the days the deal is on for.
      obj.days = 1.0
      if deal['startAt']:
        start_date = datetime.datetime.strptime(deal['startAt'], DATE_PATTERN)
        if deal['endAt']:
          end_date = datetime.datetime.strptime(deal['endAt'], DATE_PATTERN)
          delta = end_date - start_date
          xdays = round((delta.days * SECONDS_IN_DAY + delta.seconds)/SECONDS_IN_DAY)
          if xdays > 1:
            obj.days = xdays
            logging.info('more than 1 %s days: %f', obj.url, obj.days)
      obj.price = deal['options'][0]['price']['amount']/100.0
      obj.currency = deal['options'][0]['price']['currencyCode']
      obj.revenue = (obj.quantity_sold * obj.price)/obj.days
      total_revenue += obj.revenue
      objects.append(obj)
      
  db.put(objects)

  revenue_obj = Revenue()
  revenue_obj.sync_time = sync_time
  revenue_obj.revenue = total_revenue
  logging.info('Total revenue ... %f', total_revenue)
  db.put(revenue_obj)
  
  sync_obj = Syncs()
  sync_obj.sync_time = sync_time
  db.put(sync_obj)

def LastUpdated():
  results = db.GqlQuery("SELECT * FROM Syncs ORDER BY date DESC LIMIT 1")
  for result in results:
    return result.date

######################################################################
##
##      Class Handlers
##
######################################################################

class All(webapp.RequestHandler):
  def get(self):
    sync_list = []
    revenue_list = []
    render = []
    syncs = db.GqlQuery("SELECT * FROM Syncs ORDER BY date DESC LIMIT 500")
    path = os.path.join(os.path.dirname(__file__), 'home.html')
    template_values = {
        'syncs' : syncs,
        'last_updated' : LastUpdated(),
        }
    render_page = template.render(path, template_values)
    self.response.out.write(render_page)


class Home(webapp.RequestHandler):
  def get(self):
    sync_list = []
    revenue_list = []
    render = []
    syncs = db.GqlQuery("SELECT * FROM Syncs ORDER BY date DESC LIMIT 1000")
    sync_keys = []
    for sync in syncs:
      sync_key = sync.sync_time[:sync.sync_time.find(' ')]
      if sync_key not in sync_keys:
        sync_keys.append(sync_key)
    path = os.path.join(os.path.dirname(__file__), 'day.html')
    template_values = {
        'syncs' : sync_keys,
        'last_updated' : LastUpdated(),
        }
    render_page = template.render(path, template_values)
    self.response.out.write(render_page)

class SyncReport(webapp.RequestHandler):
  def get(self, sync_time=None):
    if sync_time:
      sync_time = unicode(urllib.unquote(sync_time), 'utf-8')
      st = urllib.unquote_plus(sync_time)
      deals = db.GqlQuery("SELECT * FROM Deal WHERE sync_time = :tt ORDER BY revenue DESC", tt=st)
      total_revenue = 0.0
      for deal in deals:
        if deal.tipped:
          total_revenue += (deal.quantity_sold * deal.price)
      path = os.path.join(os.path.dirname(__file__), 'sync_report.html')
      template_values = {
        'deals' : deals,
        'sync_time' : sync_time,
        'total_revenue' : commaify(int(total_revenue)),
        'last_updated' : LastUpdated(),
        }
      render_page = template.render(path, template_values)
      self.response.out.write(render_page)


class DayReport(webapp.RequestHandler):
  def get(self, day=None):
    found = False
    if day:
      day = unicode(urllib.unquote(day), 'utf-8')
      day = urllib.unquote_plus(day)

      # Get all deals with all sync_times
      for sync in db.GqlQuery("SELECT * FROM Syncs ORDER BY date DESC"):
        if day in sync.sync_time:
          deals = db.GqlQuery("SELECT * FROM Deal WHERE sync_time = :tt ORDER BY revenue DESC", tt=sync.sync_time)
          total_revenue = 0.0
          for deal in deals:
            if deal.tipped:
              total_revenue += (deal.quantity_sold * deal.price)
          path = os.path.join(os.path.dirname(__file__), 'sync_report.html')
          template_values = {
              'deals' : deals,
              'sync_time' : sync.sync_time,
              'total_revenue' : commaify(int(total_revenue)),
              'last_updated' : LastUpdated(),
              }
          render_page = template.render(path, template_values)
          self.response.out.write(render_page)
          found = True
          break

    if not found:
      self.redirect('/', permanent=False)


class Cron(webapp.RequestHandler):
  def get(self):
    Process()

class CronDelete(webapp.RequestHandler):
  def get(self):
    syncs = db.GqlQuery("SELECT * FROM Syncs ORDER BY date DESC")
    # Get a list of all the syncs that are the latest for that day
    days = []
    old_sync_keys = []
    for sync in syncs:
      day = sync.sync_time[:sync.sync_time.find(' ')]
      if day not in days:
        days.append(day)
      else:
        old_sync_keys.append(sync.sync_time)
        
    logging.info('Deleting %d records', len(old_sync_keys))

    # Data belonging to all old sync keys can be deleted.
    for st in old_sync_keys[:30]:
      logging.info('Deleting ... %s', st)
      results=db.GqlQuery("SELECT * FROM Deal WHERE sync_time = :tt", tt=st)
      for result in results:
        result.delete()
      results=db.GqlQuery("SELECT * FROM Syncs WHERE sync_time = :tt", tt=st)
      for result in results:
        result.delete()
      results=db.GqlQuery("SELECT * FROM Revenue WHERE sync_time = :tt", tt=st)
      for result in results:
        result.delete()

class Feedback(webapp.RequestHandler):
  def get(self):
    path = os.path.join(os.path.dirname(__file__), 'feedback.html')
    template_values = {
        'last_updated' : LastUpdated(),
        }
    render_page = template.render(path, template_values)
    self.response.out.write(render_page)

    
class About(webapp.RequestHandler):
  def get(self):
    path = os.path.join(os.path.dirname(__file__), 'about.html')
    template_values = {
        'last_updated' : LastUpdated(),
        }
    render_page = template.render(path, template_values)
    self.response.out.write(render_page)
    
def main():
  application = webapp.WSGIApplication([
      ('/', Home),
      ('/about', About),
      ('/feedback', Feedback),
      ('/cron', Cron),
      ('/crondelete', CronDelete),
      (r'/day/(.*)', DayReport),
      # Not shown to the public
      ('/all', All),
      (r'/sync/(.*)', SyncReport),
      ], debug=True)
  wsgiref.handlers.CGIHandler().run(application)

if __name__ == '__main__':
  main()
