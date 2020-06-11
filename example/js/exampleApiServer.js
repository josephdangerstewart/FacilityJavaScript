// DO NOT EDIT: generated by fsdgenjs
/* eslint-disable */
'use strict';

import * as bodyParser from 'body-parser';
import * as express from 'express';

const standardErrorCodes = {
  'NotModified': 304,
  'InvalidRequest': 400,
  'NotAuthenticated': 401,
  'NotAuthorized': 403,
  'NotFound': 404,
  'Conflict': 409,
  'RequestTooLarge': 413,
  'TooManyRequests': 429,
  'InternalError': 500,
  'ServiceUnavailable': 503,
};

function parseBoolean(value) {
  if (typeof value === 'string') {
    const lowerValue = value.toLowerCase();
    if (lowerValue === 'true') {
      return true;
    }
    if (lowerValue === 'false') {
      return false;
    }
  }
  return undefined;
}

export function createApp(service) {
  const app = express();
  app.use(bodyParser.json());
  app.use(bodyParser.urlencoded({ extended: true }));

  /** Gets widgets. */
  app.get('/widgets', function (req, res, next) {
    const request = {};
    if (req.query['q'] != null) {
      request.query = req.query['q'];
    }
    if (req.query['limit'] != null) {
      request.limit = parseInt(req.query['limit'], 10);
    }
    if (req.query['sort'] != null) {
      request.sort = req.query['sort'];
    }
    if (req.query['desc'] != null) {
      request.desc = parseBoolean(req.query['desc']);
    }
    if (req.query['maxWeight'] != null) {
      request.maxWeight = parseFloat(req.query['maxWeight']);
    }
    if (req.query['minPrice'] != null) {
      request.minPrice = parseFloat(req.query['minPrice']);
    }

    return service.getWidgets(request)
      .then(result => {
        if (result.error) {
          const status = result.error.code && standardErrorCodes[result.error.code] || 500;
          res.status(status).send(result.error);
          return;
        }
        if (result.value) {
          if (result.value.job) {
            res.status(202).send(result.value.job);
            return;
          }
          res.status(200).send({
            widgets: result.value.widgets,
            total: result.value.total,
            totalWeight: result.value.totalWeight,
          });
          return;
        }
        throw new Error('Result must have an error or value.');
      })
      .catch(next);
  });

  /** Creates a new widget. */
  app.post('/widgets', function (req, res, next) {
    const request = {};
    request.widget = req.body;

    return service.createWidget(request)
      .then(result => {
        if (result.error) {
          const status = result.error.code && standardErrorCodes[result.error.code] || 500;
          res.status(status).send(result.error);
          return;
        }
        if (result.value) {
          if (result.value.widget) {
            res.status(201).send(result.value.widget);
            return;
          }
        }
        throw new Error('Result must have an error or value.');
      })
      .catch(next);
  });

  /** Gets the specified widget. */
  app.get('/widgets/:id', function (req, res, next) {
    const request = {};
    request.id = req.params.id;
    request.ifNoneMatch = req.header('If-None-Match');

    return service.getWidget(request)
      .then(result => {
        if (result.error) {
          const status = result.error.code && standardErrorCodes[result.error.code] || 500;
          res.status(status).send(result.error);
          return;
        }
        if (result.value) {
          if (result.value.eTag != null) {
            res.setHeader('eTag', result.value.eTag);
          }
          if (result.value.cacheControl != null) {
            res.setHeader('Cache-Control', result.value.cacheControl);
          }
          if (result.value.widget) {
            res.status(200).send(result.value.widget);
            return;
          }
          if (result.value.notModified) {
            res.sendStatus(304);
            return;
          }
        }
        throw new Error('Result must have an error or value.');
      })
      .catch(next);
  });

  /** Deletes the specified widget. */
  app.delete('/widgets/:id', function (req, res, next) {
    const request = {};
    request.id = req.params.id;

    return service.deleteWidget(request)
      .then(result => {
        if (result.error) {
          const status = result.error.code && standardErrorCodes[result.error.code] || 500;
          res.status(status).send(result.error);
          return;
        }
        if (result.value) {
          res.sendStatus(204);
          return;
        }
        throw new Error('Result must have an error or value.');
      })
      .catch(next);
  });

  /** Edits widget. */
  app.post('/widgets/:id', function (req, res, next) {
    const request = {};
    request.id = req.params.id;
    request.ops = req.body.ops;
    request.weight = req.body.weight;

    return service.editWidget(request)
      .then(result => {
        if (result.error) {
          const status = result.error.code && standardErrorCodes[result.error.code] || 500;
          res.status(status).send(result.error);
          return;
        }
        if (result.value) {
          if (result.value.widget) {
            res.status(200).send(result.value.widget);
            return;
          }
          if (result.value.job) {
            res.status(202).send(result.value.job);
            return;
          }
        }
        throw new Error('Result must have an error or value.');
      })
      .catch(next);
  });

  /** Gets the specified widgets. */
  app.post('/widgets/get', function (req, res, next) {
    const request = {};
    request.ids = req.body;

    return service.getWidgetBatch(request)
      .then(result => {
        if (result.error) {
          const status = result.error.code && standardErrorCodes[result.error.code] || 500;
          res.status(status).send(result.error);
          return;
        }
        if (result.value) {
          if (result.value.results) {
            res.status(200).send(result.value.results);
            return;
          }
        }
        throw new Error('Result must have an error or value.');
      })
      .catch(next);
  });

  /**
   * Gets the widget weight.
   * @deprecated
   */
  app.get('/widgets/:id/weight', function (req, res, next) {
    const request = {};
    request.id = req.params.id;

    return service.getWidgetWeight(request)
      .then(result => {
        if (result.error) {
          const status = result.error.code && standardErrorCodes[result.error.code] || 500;
          res.status(status).send(result.error);
          return;
        }
        if (result.value) {
          res.status(200).send({
            value: result.value.value,
          });
          return;
        }
        throw new Error('Result must have an error or value.');
      })
      .catch(next);
  });

  /** Gets a widget preference. */
  app.get('/prefs/:key', function (req, res, next) {
    const request = {};
    request.key = req.params.key;

    return service.getPreference(request)
      .then(result => {
        if (result.error) {
          const status = result.error.code && standardErrorCodes[result.error.code] || 500;
          res.status(status).send(result.error);
          return;
        }
        if (result.value) {
          if (result.value.value) {
            res.status(200).send(result.value.value);
            return;
          }
        }
        throw new Error('Result must have an error or value.');
      })
      .catch(next);
  });

  /** Sets a widget preference. */
  app.put('/prefs/:key', function (req, res, next) {
    const request = {};
    request.key = req.params.key;
    request.value = req.body;

    return service.setPreference(request)
      .then(result => {
        if (result.error) {
          const status = result.error.code && standardErrorCodes[result.error.code] || 500;
          res.status(status).send(result.error);
          return;
        }
        if (result.value) {
          if (result.value.value) {
            res.status(200).send(result.value.value);
            return;
          }
        }
        throw new Error('Result must have an error or value.');
      })
      .catch(next);
  });

  /** Gets service info. */
  app.get('/', function (req, res, next) {
    const request = {};

    return service.getInfo(request)
      .then(result => {
        if (result.error) {
          const status = result.error.code && standardErrorCodes[result.error.code] || 500;
          res.status(status).send(result.error);
          return;
        }
        if (result.value) {
          res.status(200).send({
            name: result.value.name,
          });
          return;
        }
        throw new Error('Result must have an error or value.');
      })
      .catch(next);
  });

  /** Demonstrates the default HTTP behavior. */
  app.post('/notRestful', function (req, res, next) {
    const request = {};

    return service.notRestful(request)
      .then(result => {
        if (result.error) {
          const status = result.error.code && standardErrorCodes[result.error.code] || 500;
          res.status(status).send(result.error);
          return;
        }
        if (result.value) {
          res.sendStatus(200);
          return;
        }
        throw new Error('Result must have an error or value.');
      })
      .catch(next);
  });

  app.post('/kitchen', function (req, res, next) {
    const request = {};
    request.sink = req.body.sink;

    return service.kitchen(request)
      .then(result => {
        if (result.error) {
          const status = result.error.code && standardErrorCodes[result.error.code] || 500;
          res.status(status).send(result.error);
          return;
        }
        if (result.value) {
          res.sendStatus(200);
          return;
        }
        throw new Error('Result must have an error or value.');
      })
      .catch(next);
  });

  return app;
}
