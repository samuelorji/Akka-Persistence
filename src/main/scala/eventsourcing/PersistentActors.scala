package eventsourcing

import java.util.Date

import akka.actor.{ActorLogging, ActorSystem, PoisonPill, Props}
import akka.persistence.PersistentActor

object PersistentActors extends App {





  case class Invoice(recipient : String, date : Date , amount : Int)
  case class InvoiceRecorded(id : Int, recipient : String, date : Date , amount : Int)
  case class Invoices(invoices : List[Invoice])

  case object Shutdown


  class Accountant extends PersistentActor with ActorLogging{

    var latestInvoiceId = 0
    var totalAmount     = 0
    override def persistenceId: String = "accountant"

    override def receiveRecover: Receive = {
      case req : InvoiceRecorded =>
        latestInvoiceId += req.id
        totalAmount     += req.amount
        log.info(s"recovered $req from Store, total amount is now $totalAmount ")
    }

    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Persistence failed on ${event} because of ${cause}")
      super.onPersistFailure(cause, event, seqNr)
    }

    override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Persistence failed on ${event} because of ${cause}")
      super.onPersistRejected(cause, event, seqNr)
    }
    override def receiveCommand: Receive = {
      case req : Invoice =>
        //val currentSender = sender()
        log.info(s"received Invoice : [$req]")

        //let us create our event that we will then persist

      persist(InvoiceRecorded(latestInvoiceId,req.recipient,req.date,req.amount)){ev =>
        latestInvoiceId += 1
        totalAmount += req.amount
        log.info(s"persisted event ${req}, total amount is now $totalAmount")
      }


      case Shutdown =>
        println("shutting down persistent actor")
        context stop self
      case req : Invoices =>

        val eventsId = latestInvoiceId to (latestInvoiceId + req.invoices.size)

        val events = req.invoices zip eventsId map {
          case (inv, id) =>
            InvoiceRecorded(id, inv.recipient, inv.date, inv.amount)
        }

        persistAll(events){ev =>
          latestInvoiceId += 1
          totalAmount     += ev.amount
          log.info(s"Persisted event ${ev}, total amount is now ${totalAmount + ev.amount}")
        }
    }
  }
  implicit val system = ActorSystem("Persistence")
  val accountant = system.actorOf(Props[Accountant])
 // for (_ <- 1 to 10) accountant ! Invoice("JAAC Motors",new Date,1000)

  val events = 1 to 10 map(x => Invoice("JAC Motors", new Date,1000 * x))

  accountant ! Invoices(events.toList)

  //Best practice to close down actors
  accountant ! Shutdown
}
