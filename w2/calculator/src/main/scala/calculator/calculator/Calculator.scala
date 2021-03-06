package calculator


final case class Literal(v: Double) extends Expr

final case class Ref(name: String) extends Expr

final case class Plus(a: Expr, b: Expr) extends Expr

final case class Minus(a: Expr, b: Expr) extends Expr

final case class Times(a: Expr, b: Expr) extends Expr

final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator {
  def computeValues(namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] =
    namedExpressions map {
      case (str, expr) => str -> Signal(eval(expr(), namedExpressions))
    }

  def eval(expr: Expr, references: Map[String, Signal[Expr]]): Double = expr match {
    case Literal(l) => l
    case Ref(n) => eval(getReferenceExpr(n, references), references - n)
    case Plus(lhs, rhs) => eval(lhs, references) + eval(rhs, references)
    case Minus(lhs, rhs) => eval(lhs, references) - eval(rhs, references)
    case Times(lhs, rhs) => eval(lhs, references) * eval(rhs, references)
    case Divide(lhs, rhs) => eval(lhs, references) / eval(rhs, references)
  }

  /** Get the Expr for a referenced variables.
    * If the variable is not known, returns a literal NaN.
    */
  private def getReferenceExpr(name: String,
                               references: Map[String, Signal[Expr]]) = {
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
