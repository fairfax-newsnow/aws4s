package org.aws4s.kms

import cats.effect.Effect
import io.circe.{Decoder, Json}
import org.aws4s.core.ExtraEntityDecoderInstances._
import org.aws4s._
import org.aws4s.core.{Command, CommandPayload, RenderedParam, ServiceName}
import org.http4s.headers.{Host, `Content-Type`}
import org.http4s.{Header, Headers, MediaType, Method, Request, Uri}

private[kms] abstract class KmsCommand[F[_]: Effect, R: Decoder] extends Command[F, Json, R] {
  override def serviceName:    ServiceName    = ServiceName.Kms
  override def payloadSigning: PayloadSigning = PayloadSigning.Signed

  def action: String

  override final val requestGenerator: List[RenderedParam[Json]] => F[Request[F]] = { params =>
    val host = s"kms.${region.name}.amazonaws.com"
    val payload: Json = CommandPayload.jsonObject(params)
    Effect[F].delay(Request[F](
      Method.POST,
      Uri.unsafeFromString(s"https://$host/"),
      headers = Headers(
        Header("X-Amz-Target", s"TrentService.$action"),
        Host(host),
        `Content-Type`.apply(MediaType.parse("application/x-amz-json-1.1").toOption.get)
      ),
      body = fs2.Stream(payload.noSpaces).through(fs2.text.utf8Encode)
    ))
  }
}
