package ch.post.labs.kstreams.bank.model

data class AtmEvent(
    val id: String,
    val timestamp: Long,
    val withdrawn: AtmTransaction?,
    val deposit: AtmTransaction?,
)

data class AtmTransaction(val amount: Long, val atm: String)
