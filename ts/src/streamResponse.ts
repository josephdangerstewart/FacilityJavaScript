export async function* streamResponse(body: ReadableStream<Uint8Array<ArrayBuffer>>) {
	const reader = body.pipeThrough(new TextDecoderStream()).getReader();
	if (!reader) {
		throw new Error('Response body is not readable');
	}

	const eventBuilder = new ServerSentEventBuilder();
	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) {
				break;
			}

			eventBuilder.ingestStream(value);

			for (const data of eventBuilder.flushPublishedEvents()) {
				yield data;
			}
		}
	} finally {
		reader.cancel();
	}
}

export interface IServerSentEvent {
	event: string;
	data: string;
	id?: string;
}

class ServerSentEventBuilder {
	private publishedEvents: IServerSentEvent[] = [];
	private currentEvent: IServerSentEvent | undefined;
	private streamBuffer: BufferedStream = new BufferedStream();

	public flushPublishedEvents = (): readonly Readonly<IServerSentEvent>[] => {
		const publishedEvents = this.publishedEvents;
		this.publishedEvents = [];
		return publishedEvents;
	};

	public ingestStream = (stream: string) => {
		const lines = this.streamBuffer.read(stream);

		for (const line of lines) {
			if (line.startsWith(':')) {
				continue;
			} else if (line.trim() === '') {
				this.publishCurrentEvent();
			} else if (line.includes(':')) {
				this.processField(line);
			} else {
				this.appendFieldToCurrentEvent(line, '');
			}
		}
	};

	private publishCurrentEvent = () => {
		if (!this.currentEvent) {
			return;
		}

		if (this.currentEvent.data.endsWith('\n')) {
			this.currentEvent.data = this.currentEvent.data.substring(
				0,
				this.currentEvent.data.length - 1
			);
		}

		this.publishedEvents.push({ ...this.currentEvent });
		this.currentEvent = undefined;
	};

	private processField = (rawField: string) => {
		const firstColonIndex = rawField.indexOf(':');
		const fieldName = rawField.substring(0, firstColonIndex);
		let fieldValue = rawField.substring(firstColonIndex + 1);

		if (fieldValue.startsWith(' ')) {
			fieldValue = fieldValue.substring(1);
		}

		this.appendFieldToCurrentEvent(fieldName, fieldValue);
	};

	private appendFieldToCurrentEvent = (fieldName: string, fieldValue: string) => {
		this.currentEvent ??= {
			event: 'message',
			data: '',
		};

		if (fieldName === 'event') {
			this.currentEvent.event = fieldValue;
		} else if (fieldName === 'data') {
			this.currentEvent.data ??= '';
			this.currentEvent.data += fieldValue + '\n';
		} else if (fieldName === 'id') {
			this.currentEvent.id = fieldValue;
		}
	};
}

class BufferedStream {
	private buffer = '';

	public read = (stream: string) => {
		this.buffer += stream;

		const result: string[] = [];

		// eslint-disable-next-line no-constant-condition
		while (true) {
			const newLineIndex = this.buffer.indexOf('\n');
			if (newLineIndex === -1) {
				break;
			}

			const nextLine = this.buffer.substring(0, newLineIndex);
			this.buffer = this.buffer.substring(newLineIndex + 1);
			result.push(nextLine);
		}

		return result;
	};
}
