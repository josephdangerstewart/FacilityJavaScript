// DO NOT EDIT: generated by fsdgenjs
/* eslint-disable */

import { HttpClientUtility, IServiceResult, IHttpClientOptions } from 'facility-core';
import { IExampleApi, IGetWidgetsRequest, IGetWidgetsResponse, ICreateWidgetRequest, ICreateWidgetResponse, IGetWidgetRequest, IGetWidgetResponse, IDeleteWidgetRequest, IDeleteWidgetResponse, IEditWidgetRequest, IEditWidgetResponse, IGetWidgetBatchRequest, IGetWidgetBatchResponse, IGetWidgetWeightRequest, IGetWidgetWeightResponse, IGetPreferenceRequest, IGetPreferenceResponse, ISetPreferenceRequest, ISetPreferenceResponse, IGetInfoRequest, IGetInfoResponse, INotRestfulRequest, INotRestfulResponse, IKitchenRequest, IKitchenResponse, IWidget, IWidgetJob, IPreference, IObsoleteData, IKitchenSink, WidgetField, ObsoleteEnum } from './exampleApiTypes';
export * from './exampleApiTypes';

/** Provides access to ExampleApi over HTTP via fetch. */
export function createHttpClient({ fetch, baseUri }: IHttpClientOptions): IExampleApi {
	return new ExampleApiHttpClient(fetch, baseUri);
}

const { fetchResponse, createResponseError, createRequiredRequestFieldError } = HttpClientUtility;
type IFetch = HttpClientUtility.IFetch;
type IFetchRequest = HttpClientUtility.IFetchRequest;

class ExampleApiHttpClient implements IExampleApi {
	constructor(fetch: IFetch, baseUri?: string) {
		if (typeof fetch !== 'function') {
			throw new TypeError('fetch must be a function.');
		}
		if (typeof baseUri === 'undefined') {
			baseUri = 'http://local.example.com/v1';
		}
		if (/[^\/]$/.test(baseUri)) {
			baseUri += '/';
		}
		this._fetch = fetch;
		this._baseUri = baseUri;
	}

	/** Gets widgets. */
	public getWidgets(request: IGetWidgetsRequest, context?: unknown): Promise<IServiceResult<IGetWidgetsResponse>> {
		let uri = 'widgets';
		const query: string[] = [];
		request.query == null || query.push('q=' + encodeURIComponent(request.query));
		request.limit == null || query.push('limit=' + request.limit.toString());
		request.sort == null || query.push('sort=' + request.sort);
		request.desc == null || query.push('desc=' + request.desc.toString());
		request.maxWeight == null || query.push('maxWeight=' + encodeURIComponent(request.maxWeight.toString()));
		request.minPrice == null || query.push('minPrice=' + request.minPrice.toString());
		if (query.length) {
			uri = uri + '?' + query.join('&');
		}
		const fetchRequest: IFetchRequest = {
			method: 'GET',
		};
		return fetchResponse(this._fetch, this._baseUri + uri, fetchRequest, context)
			.then(result => {
				const status = result.response.status;
				let value: IGetWidgetsResponse | null = null;
				if (status === 200 && result.json) {
					value = result.json as IGetWidgetsResponse | null;
				}
				else if (status === 202 && result.json) {
					value = { job: result.json } as IGetWidgetsResponse;
				}
				if (!value) {
					return createResponseError(status, result.json) as IServiceResult<IGetWidgetsResponse>;
				}
				return { value: value };
			});
	}

	/** Creates a new widget. */
	public createWidget(request: ICreateWidgetRequest, context?: unknown): Promise<IServiceResult<ICreateWidgetResponse>> {
		const uri = 'widgets';
		const fetchRequest: IFetchRequest = {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(request.widget)
		};
		return fetchResponse(this._fetch, this._baseUri + uri, fetchRequest, context)
			.then(result => {
				const status = result.response.status;
				let value: ICreateWidgetResponse | null = null;
				if (status === 201 && result.json) {
					value = { widget: result.json } as ICreateWidgetResponse;
				}
				if (!value) {
					return createResponseError(status, result.json) as IServiceResult<ICreateWidgetResponse>;
				}
				return { value: value };
			});
	}

	/** Gets the specified widget. */
	public getWidget(request: IGetWidgetRequest, context?: unknown): Promise<IServiceResult<IGetWidgetResponse>> {
		const uriPartId = request.id != null && encodeURIComponent(request.id);
		if (!uriPartId) {
			return Promise.resolve(createRequiredRequestFieldError('id'));
		}
		const uri = `widgets/${uriPartId}`;
		const fetchRequest: IFetchRequest = {
			method: 'GET',
			headers: {},
		};
		if (request.ifNoneMatch != null) {
			fetchRequest.headers!['If-None-Match'] = request.ifNoneMatch;
		}
		return fetchResponse(this._fetch, this._baseUri + uri, fetchRequest, context)
			.then(result => {
				const status = result.response.status;
				let value: IGetWidgetResponse | null = null;
				if (status === 200 && result.json) {
					value = { widget: result.json } as IGetWidgetResponse;
				}
				else if (status === 304) {
					value = { notModified: true };
				}
				if (!value) {
					return createResponseError(status, result.json) as IServiceResult<IGetWidgetResponse>;
				}
				let headerValue: string | null | undefined;
				headerValue = result.response.headers.get('eTag');
				if (headerValue != null) {
					value.eTag = headerValue;
				}
				headerValue = result.response.headers.get('Cache-Control');
				if (headerValue != null) {
					value.cacheControl = headerValue;
				}
				return { value: value };
			});
	}

	/** Deletes the specified widget. */
	public deleteWidget(request: IDeleteWidgetRequest, context?: unknown): Promise<IServiceResult<IDeleteWidgetResponse>> {
		const uriPartId = request.id != null && encodeURIComponent(request.id);
		if (!uriPartId) {
			return Promise.resolve(createRequiredRequestFieldError('id'));
		}
		const uri = `widgets/${uriPartId}`;
		const fetchRequest: IFetchRequest = {
			method: 'DELETE',
		};
		return fetchResponse(this._fetch, this._baseUri + uri, fetchRequest, context)
			.then(result => {
				const status = result.response.status;
				let value: IDeleteWidgetResponse | null = null;
				if (status === 204 && result.json) {
					value = {};
				}
				if (!value) {
					return createResponseError(status, result.json) as IServiceResult<IDeleteWidgetResponse>;
				}
				return { value: value };
			});
	}

	/** Edits widget. */
	public editWidget(request: IEditWidgetRequest, context?: unknown): Promise<IServiceResult<IEditWidgetResponse>> {
		const uriPartId = request.id != null && encodeURIComponent(request.id);
		if (!uriPartId) {
			return Promise.resolve(createRequiredRequestFieldError('id'));
		}
		const uri = `widgets/${uriPartId}`;
		const fetchRequest: IFetchRequest = {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				ops: request.ops,
				weight: request.weight
			})
		};
		return fetchResponse(this._fetch, this._baseUri + uri, fetchRequest, context)
			.then(result => {
				const status = result.response.status;
				let value: IEditWidgetResponse | null = null;
				if (status === 200 && result.json) {
					value = { widget: result.json } as IEditWidgetResponse;
				}
				else if (status === 202 && result.json) {
					value = { job: result.json } as IEditWidgetResponse;
				}
				if (!value) {
					return createResponseError(status, result.json) as IServiceResult<IEditWidgetResponse>;
				}
				return { value: value };
			});
	}

	/** Gets the specified widgets. */
	public getWidgetBatch(request: IGetWidgetBatchRequest, context?: unknown): Promise<IServiceResult<IGetWidgetBatchResponse>> {
		const uri = 'widgets/get';
		const fetchRequest: IFetchRequest = {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(request.ids)
		};
		return fetchResponse(this._fetch, this._baseUri + uri, fetchRequest, context)
			.then(result => {
				const status = result.response.status;
				let value: IGetWidgetBatchResponse | null = null;
				if (status === 200 && result.json) {
					value = { results: result.json } as IGetWidgetBatchResponse;
				}
				if (!value) {
					return createResponseError(status, result.json) as IServiceResult<IGetWidgetBatchResponse>;
				}
				return { value: value };
			});
	}

	/**
	 * Gets the widget weight.
	 * @deprecated
	 */
	public getWidgetWeight(request: IGetWidgetWeightRequest, context?: unknown): Promise<IServiceResult<IGetWidgetWeightResponse>> {
		const uriPartId = request.id != null && encodeURIComponent(request.id);
		if (!uriPartId) {
			return Promise.resolve(createRequiredRequestFieldError('id'));
		}
		const uri = `widgets/${uriPartId}/weight`;
		const fetchRequest: IFetchRequest = {
			method: 'GET',
		};
		return fetchResponse(this._fetch, this._baseUri + uri, fetchRequest, context)
			.then(result => {
				const status = result.response.status;
				let value: IGetWidgetWeightResponse | null = null;
				if (status === 200 && result.json) {
					value = result.json as IGetWidgetWeightResponse | null;
				}
				if (!value) {
					return createResponseError(status, result.json) as IServiceResult<IGetWidgetWeightResponse>;
				}
				return { value: value };
			});
	}

	/** Gets a widget preference. */
	public getPreference(request: IGetPreferenceRequest, context?: unknown): Promise<IServiceResult<IGetPreferenceResponse>> {
		const uriPartKey = request.key != null && encodeURIComponent(request.key);
		if (!uriPartKey) {
			return Promise.resolve(createRequiredRequestFieldError('key'));
		}
		const uri = `prefs/${uriPartKey}`;
		const fetchRequest: IFetchRequest = {
			method: 'GET',
		};
		return fetchResponse(this._fetch, this._baseUri + uri, fetchRequest, context)
			.then(result => {
				const status = result.response.status;
				let value: IGetPreferenceResponse | null = null;
				if (status === 200 && result.json) {
					value = { value: result.json } as IGetPreferenceResponse;
				}
				if (!value) {
					return createResponseError(status, result.json) as IServiceResult<IGetPreferenceResponse>;
				}
				return { value: value };
			});
	}

	/** Sets a widget preference. */
	public setPreference(request: ISetPreferenceRequest, context?: unknown): Promise<IServiceResult<ISetPreferenceResponse>> {
		const uriPartKey = request.key != null && encodeURIComponent(request.key);
		if (!uriPartKey) {
			return Promise.resolve(createRequiredRequestFieldError('key'));
		}
		const uri = `prefs/${uriPartKey}`;
		const fetchRequest: IFetchRequest = {
			method: 'PUT',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(request.value)
		};
		return fetchResponse(this._fetch, this._baseUri + uri, fetchRequest, context)
			.then(result => {
				const status = result.response.status;
				let value: ISetPreferenceResponse | null = null;
				if (status === 200 && result.json) {
					value = { value: result.json } as ISetPreferenceResponse;
				}
				if (!value) {
					return createResponseError(status, result.json) as IServiceResult<ISetPreferenceResponse>;
				}
				return { value: value };
			});
	}

	/** Gets service info. */
	public getInfo(request: IGetInfoRequest, context?: unknown): Promise<IServiceResult<IGetInfoResponse>> {
		const uri = '';
		const fetchRequest: IFetchRequest = {
			method: 'GET',
		};
		return fetchResponse(this._fetch, this._baseUri + uri, fetchRequest, context)
			.then(result => {
				const status = result.response.status;
				let value: IGetInfoResponse | null = null;
				if (status === 200 && result.json) {
					value = result.json as IGetInfoResponse | null;
				}
				if (!value) {
					return createResponseError(status, result.json) as IServiceResult<IGetInfoResponse>;
				}
				return { value: value };
			});
	}

	/** Demonstrates the default HTTP behavior. */
	public notRestful(request: INotRestfulRequest, context?: unknown): Promise<IServiceResult<INotRestfulResponse>> {
		const uri = 'notRestful';
		const fetchRequest: IFetchRequest = {
			method: 'POST',
		};
		return fetchResponse(this._fetch, this._baseUri + uri, fetchRequest, context)
			.then(result => {
				const status = result.response.status;
				let value: INotRestfulResponse | null = null;
				if (status === 200 && result.json) {
					value = {};
				}
				if (!value) {
					return createResponseError(status, result.json) as IServiceResult<INotRestfulResponse>;
				}
				return { value: value };
			});
	}

	public kitchen(request: IKitchenRequest, context?: unknown): Promise<IServiceResult<IKitchenResponse>> {
		const uri = 'kitchen';
		const fetchRequest: IFetchRequest = {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(request)
		};
		return fetchResponse(this._fetch, this._baseUri + uri, fetchRequest, context)
			.then(result => {
				const status = result.response.status;
				let value: IKitchenResponse | null = null;
				if (status === 200 && result.json) {
					value = {};
				}
				if (!value) {
					return createResponseError(status, result.json) as IServiceResult<IKitchenResponse>;
				}
				return { value: value };
			});
	}

	private _fetch: IFetch;
	private _baseUri: string;
}
