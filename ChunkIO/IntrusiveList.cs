using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  class IntrusiveListNode<T> where T : IntrusiveListNode<T> {
    public T Prev { get; internal set; }
    public T Next { get; internal set; }

    // This weird nesting is the only way I know how to ensure that only the list can
    // mutate Prev and Next in the node. WTB friend classes.
    public class List {
      public void AddLast(T node) {
        Debug.Assert(node != null);
        Debug.Assert(node.Prev == null);
        Debug.Assert(node.Next == null);
        if (Last != null) {
          Debug.Assert(First != null);
          Debug.Assert(Last.Next == null);
          Last.Next = node;
        } else {
          Debug.Assert(First == null);
          First = node;
        }
        node.Prev = Last;
        node.Next = null;
        Last = node;
        Debug.Assert(First != null && Last != null);
      }

      public bool IsLinked(T node) {
        Debug.Assert(node != null);
        if (node.Prev != null || node.Next != null) {
          Debug.Assert(First != null && Last != null);
          return true;
        }
        if (First == node) {
          Debug.Assert(Last == node);
          return true;
        }
        Debug.Assert(Last != node);
        return false;
      }

      public void Remove(T node) {
        Debug.Assert(node != null);
        T prev = node.Prev;
        T next = node.Next;
        if (prev != null) {
          Debug.Assert(First != node);
          Debug.Assert(prev.Next == node);
          prev.Next = next;
          node.Prev = null;
        } else {
          Debug.Assert(First == node);
          First = next;
        }
        if (next != null) {
          Debug.Assert(Last != node);
          Debug.Assert(next.Prev == node);
          next.Prev = prev;
          node.Next = null;
        } else {
          Debug.Assert(Last == node);
          Last = prev;
        }
        Debug.Assert((First == null) == (Last == null));
      }

      // Null iff the list is empty.
      public T First { get; internal set; }

      // Null iff the list is empty.
      public T Last { get; internal set; }
    }
  }
}
