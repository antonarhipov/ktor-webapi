package kong.builders

import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.core.data.PojoCloudEventData
import io.cloudevents.core.message.MessageReader
import io.cloudevents.core.provider.EventFormatProvider
import io.cloudevents.http.HttpMessageFactory
import io.cloudevents.http.impl.HttpMessageWriter
import io.cloudevents.jackson.JsonFormat
import io.cloudevents.jackson.PojoCloudEventDataMapper
import io.cloudevents.rw.CloudEventDataMapper
import io.ktor.http.*
import io.ktor.serialization.jackson.*
import io.ktor.server.application.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.util.*
import kong.builders.plugins.configureSockets
import kotlinx.coroutines.runBlocking
import java.net.URI
import java.time.OffsetDateTime
import java.util.*
import java.util.function.BiConsumer

fun main(args: Array<String>): Unit =
    io.ktor.server.netty.EngineMain.main(args)

data class Event(val id: String, val text: String)

val events = mutableMapOf<String, Event>()
var eventFormat = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE)

fun Application.module() {
    install(ContentNegotiation) {
        jackson {}
    }

    routing {
        // return all events
        get("/event") {
            val cloudEvent = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("localhost"))
                .withTime(OffsetDateTime.now())
                .withType("myEventType")
                // this is a custom extension function from Jackson (needs to be imported)
                // withData that parametrised with type of event
                // use
                //.withData(Event("10", URI.create("foobar").toString()))
                .withData("test".toByteArray())
                .build()

            getMessageWriter(call).writeStructured(cloudEvent, eventFormat)

        }
        //return event by id
        get("/event/{id}") {
            val id = call.parameters["id"]
            val event = events[id]
            if (event != null) {
                call.respond(event)
            } else {
                call.respond(HttpStatusCode.NotFound, Event("", "arghhh!!!"))
            }
        }
        // post an event
        post("/event/") {

            var cloudEvent = eventFormat.deserialize(call.request.receiveChannel().toByteArray())
            println(cloudEvent)
            /*val ce = getMessageReader(call).toEvent()

            println(ce)
            //getMessageWriter(call).writeBinary(ce)
            *//*
                val event = call.receive<Event>()
                events[event.id] = event
                */
            call.response.status(HttpStatusCode.Created)
        }
    }
    configureSockets()
}

suspend fun getMessageReader(call: ApplicationCall): MessageReader {
    return HttpMessageFactory.createReader({ processHeader ->
        call.request.headers.flattenForEach { header, value -> processHeader.accept(header, value) }
    }, call.request.receiveChannel().toByteArray());
}

fun getMessageWriter(call: ApplicationCall): HttpMessageWriter {
    var contentType = ContentType.Any
    val putHeader = BiConsumer<String, String> { header, value ->
        if (header.equals("Content-Type", true)) {
            contentType = ContentType.parse(value)
        } else {
            call.response.header(header, value)
        }
    }
    return HttpMessageFactory.createWriter(putHeader) { body ->
        runBlocking {
            call.respondBytes(body, contentType, HttpStatusCode.OK)
        }
    }
}
