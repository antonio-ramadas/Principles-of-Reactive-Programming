package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("gen1") = forAll { h: H =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h))==m
  }

  // If you insert any two elements into an empty heap,
  //  finding the minimum of the resulting heap should get the smallest of the two elements back
  property("empty_heap_with_2_elements") = forAll { (i: Int, j: Int) =>
    val h = insert(i, insert(j, empty))

    val res1 = findMin(h) == (if (i < j) i else j)

    val newH = deleteMin(h)

    res1 && findMin(newH) == (if (i > j) i else j) && isEmpty(deleteMin(newH))
  }

  // If you insert an element into an empty heap,
  //  then delete the minimum, the resulting heap should be empty.
  property("empty_heap_insert_delete") = forAll { i: Int =>
    isEmpty(deleteMin(insert(i, empty)))
  }

  // Given any heap, you should get a sorted sequence of elements when continually finding and deleting minima.
  // (Hint: recursion and helper functions are your friends.)
  property("sorted_heap") = forAll { h: H =>
    next(h)
  }

  property("sorted_heap_2") = forAll { l :List[Int] =>
    val h = fill(empty, l)

    next(h) && l.sorted == getAll(h)
  }

  // Finding a minimum of the melding of any two heaps should return a minimum of one or the other.
  property("melding") = forAll { (h1: H, h2: H) =>
    val minimumH1 = findMin(h1)
    val minimumH2 = findMin(h2)

    findMin(meld(h1, h2)) == (if (minimumH1 < minimumH2) minimumH1 else minimumH2)
  }

  property("heap_size") = forAll { l: List[Int] =>
    val h = fill(empty, l)

    l.size == size(h)
  }

  def getAll(h: H): List[Int] = if (isEmpty(h)) Nil else findMin(h) :: getAll(deleteMin(h))

  def size(h: H): Int = if (isEmpty(h)) 0 else 1 + size(deleteMin(h))

  def fill(h: H, l: List[Int]) = l.foldLeft(h)((acc, i) => insert(i, acc))

  def next(h: H): Boolean = {

    def getMin(elem: Int, h: H): Int = if (isEmpty(h)) elem else findMin(h)

    if (isEmpty(h)) {
      true
    } else {
      val min = findMin(h)
      val newH = deleteMin(h)

      min <= getMin(min, newH) && (if (isEmpty(newH)) true else next(newH))
    }
  }

  lazy val genMap: Gen[Map[Int,Int]] = for {
    k <- arbitrary[Int]
    v <- arbitrary[Int]
    m <- oneOf(const(Map.empty[Int,Int]), genMap)
  } yield m.updated(k, v)

  lazy val genHeap: Gen[H] = for {
    e <- arbitrary[Int]
    h <- oneOf(const(empty), genHeap)
  } yield insert(e, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
