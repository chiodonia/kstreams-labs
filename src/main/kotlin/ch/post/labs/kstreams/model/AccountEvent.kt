package ch.post.labs.kstreams.model

data class AccountEvent(
    val id: String = "",
    val timestamp: Long = System.currentTimeMillis(),
    val withdrawn: AccountTransaction?,
    val deposit: AccountTransaction?,
)

data class AccountTransaction(val amount: Long, val atm: String)
