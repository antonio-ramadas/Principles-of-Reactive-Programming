package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Var(b()*b() - 4*a()*c())
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {

    def solution(sign: Int): Double = {
      (-b() + sign * math.sqrt(delta())) / (2 * a())
    }

    Var {
      if (delta() < 0)
        Set()
      else {
        Set(solution(1), solution(-1))
      }
    }
  }
}
