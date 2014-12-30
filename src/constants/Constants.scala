package constants

trait Constants {
  //#Window size of interval
  val RoundInterval = 8
  //#Push sum error for last 3 messages before actor shuts down
  val PushSumError = 0.0000000001
  //#Minimum gossips to be heard by actor before shutdown
  val MinimumGossipsHeard = 1
}