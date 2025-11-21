package v6

import (
	"cmp"
	"fmt"
)

type Color bool

const (
	Red		Color = true
	Black	Color = false
)

type Node[T cmp.Ordered] struct {
	Data		T
	Color		Color	// red or black
	isLeaf	bool	// true for NIL leaves
	Left		*Node[T]
	Right		*Node[T]
	Parent	*Node[T]
}

// No adjacent red nodes
// Root is always black
// Leafs are always black
type RedBlackTree[T cmp.Ordered] struct {
	Leaf	*Node[T]
	Root	*Node[T]
}

func NewLeaf[T cmp.Ordered]() *Node[T] {
	zero := new(T)	// Zero value
	return &Node[T]{
		Data:		*zero,
		Color:	Black,	// Leafs are black
		isLeaf:	true,		// true for Leafs
		Left:		nil,
		Right:	nil,
		Parent:	nil,
	}
}


func NewNode[T cmp.Ordered](data T, color Color) *Node[T] {
	// Color value defaults to red unless its black
	if color == Black {
		// Explicit check
	} else {
		color = Red
	}

	return &Node[T]{
		Data:		data,
		Color:	color,
		isLeaf:	false,	// not a leaf
		Left:		nil,
		Right:	nil,
		Parent:	nil,
	}
}

func NewTree[T cmp.Ordered]() *RedBlackTree[T] {
	leafNode := NewLeaf[T]()
	return &RedBlackTree[T]{
		Leaf:	leafNode,
		Root: leafNode,
	}
}

// MOVES THE RIGHT CHILD OF A NODE UP, 
// makes the node the left child of its previous right child
func (t *RedBlackTree[T]) RotateLeft(x *Node[T]) {
	// Set Y as the right child of X
	y := x.Right

	// Shift Y left subtree to become the right subtree of X
	x.Right = y.Left

	// If Y has a left child thats not a leaf, update the parent to X
	if y.Left != t.Leaf {
		y.Left.Parent = x
	}

	// Link Y parent to X parent
	y.Parent = x.Parent

	// If X was the root, update the root to Y
	if x.Parent == nil {
		t.Root = y
	// If X was a child node, we update 
	// X's parent's pointer to point to Y
	} else if x == x.Parent.Left {
		x.Parent.Left = y		// if X was a left child, update it
	} else {
		x.Parent.Right = y	// if X was a right child, update it
	}

	// X becomes the left child of Y
	y.Left = x
	// Set Y as the parent of X
	x.Parent = y
}

// like RotateLeft but reverse
// MOVES THE LEFT CHILD OF A NODE UP, 
// makes the node the right child of its previous left child
func (t *RedBlackTree[T]) RotateRight(y *Node[T]) {
	// Set X as the left child of Y
	x := y.Left

	// Shift X right subtree to become the left subtree of Y
	y.Left = x.Right

	// If X has a right child thats not a leaf, update the parent to Y
	if x.Right != t.Leaf {
		x.Right.Parent = y
	}

	// Link X parent to Y parent
	x.Parent = y.Parent

	// If Y was the root, update the root to X
	if y.Parent == nil {
		t.Root = x
	// If Y was a child node, we update 
	// Y's parent's pointer to point to X
	} else if y == y.Parent.Right {
		y.Parent.Right = x		// if Y was a right child, update it
	} else {
		y.Parent.Left = x	// if Y was a left child, update it
	}

	// Y becomes the right child of X
	x.Right = y
	// Set X as the parent of y
	y.Parent = x
}

// Basic binary tree positioning + calls fixInsert to fix the tree
func (t *RedBlackTree[T]) Insert(data T) {
	newNode := NewNode(data, Red)

	// Set the node's children to be leaves by default
	newNode.Left = t.Leaf
	newNode.Right = t.Leaf

	var parent *Node[T]
	current := t.Root

	// Find the correct position for the node in the tree
	for current != t.Leaf {
		parent = current
		if newNode.Data < current.Data {
			current = current.Left
		} else {
			current = current.Right
		}
	}

	newNode.Parent = parent
	if parent == nil {		// Empty tree
		t.Root = newNode
	} else if newNode.Data < parent.Data {
		parent.Left = newNode
	} else {
		parent.Right = newNode
	}

	t.fixInsert(newNode)
}

// Red Black Tree conditions
func (t *RedBlackTree[T]) fixInsert(z *Node[T]) {
	// Runs as long as we have a parent and the parent is red
	for z.Parent != nil && z.Parent.Color == Red {

		// Parent is the left child of the grandparent
		grandparent := z.Parent.Parent
		if z.Parent == grandparent.Left {

			// Right child of the grandparent is the uncle node
			uncle := grandparent.Right
			if uncle.Color == Red {
				z.Parent.Color 		= Black
				uncle.Color 			= Black
				grandparent.Color = Red

				// Set node to be the grandparent and switch colors up the tree
				z = grandparent

			// Uncle is black
			} else {
				// Inserted node is a right child
				if z == z.Parent.Right {
					z = z.Parent
					t.RotateLeft(z)
				}
				z.Parent.Color 		= Black
				grandparent.Color	= Red
				t.RotateRight(grandparent)
			}

		// Parent is right child of the grandparent
		} else {
			// Same as above but symmetric
			uncle := grandparent.Left
			if uncle.Color == Red {
				z.Parent.Color 		= Black
				uncle.Color 			= Black
				grandparent.Color = Red

				// Set node to be the grandparent and switch colors up the tree
				z = grandparent

			// Uncle is black
			} else {
				// Inserted node is a left child
				if z == z.Parent.Left {
					z = z.Parent
					t.RotateRight(z)
				}
				z.Parent.Color 		= Black
				grandparent.Color	= Red
				t.RotateLeft(grandparent)
			}
		}
	}

	t.Root.Color = Black
}

// Searches in the tree recursively
func (t *RedBlackTree[T]) Search(node *Node[T], key T) *Node[T] {
	if node.isLeaf || key == node.Data {
		return node
	}

	if key < node.Data {
		return t.Search(node.Left, key)
	}
	return t.Search(node.Right, key)
}

// Keeps moving left until it finds the leftmost node
func (t *RedBlackTree[T]) Minimum(node *Node[T]) *Node[T] {
	for !node.Left.isLeaf {
		node = node.Left
	}
	return node
}

// Moves subtree of V to the position of U
func (t *RedBlackTree[T]) Transplant(u, v *Node[T]) {
	if u.Parent == nil { // if U is the tree's root
		t.Root = v
	} else if u == u.Parent.Left {	// U is left child
		u.Parent.Left = v	// Make V the parent of U
	} else {	// U is right child
		u.Parent.Right = v
	}
	v.Parent = u.Parent
}

func (t *RedBlackTree[T]) Delete(data T) {
	z := t.Search(t.Root, data)
	if z.isLeaf {
		fmt.Printf("Value not found in tree: %v", data)
		return
	}
	
	var x *Node[T]
	y := z
	yOriginalColor := y.Color

	if z.Left.isLeaf {
		x = z.Right
		t.Transplant(z, z.Right)
	} else if z.Right.isLeaf {
		x = z.Left
		t.Transplant(z, z.Left)
	} else {
		y = t.Minimum(z.Right)
		yOriginalColor = y.Color
		x = y.Right

		if y.Parent == z {
			x.Parent = y
		} else {
			t.Transplant(y, y.Right)
			y.Right = z.Right
			y.Right.Parent = y
		}
		t.Transplant(z, y)
		y.Left = z.Left
		y.Left.Parent = y
		y.Color = z.Color
	}

	if yOriginalColor == Black {
		t.fixDelete(x)
	}
}

// Red Black tree conditions
func (t *RedBlackTree[T]) fixDelete(x *Node[T]) {
	for x != t.Root && x.Color == Black {
		if x == x.Parent.Left {
			w := x.Parent.Right
			if w.Color == Red {
				w.Color = Black
				x.Parent.Color = Red
				t.RotateLeft(x.Parent)
				w = x.Parent.Right
			}

			if w.Left.Color == Black && w.Right.Color == Black {
				w.Color = Red
				x = x.Parent
			} else {
				if w.Right.Color == Black {
					w.Left.Color = Black
					w.Color = Red
					t.RotateRight(w)
					w = x.Parent.Right
				}

				w.Color = x.Parent.Color
				x.Parent.Color = Black
				w.Right.Color = Black
				t.RotateLeft(x.Parent)
				x = t.Root
			}


		} else {
			w := x.Parent.Left
			if w.Color == Red {
				w.Color = Black
				x.Parent.Color = Red
				t.RotateRight(x.Parent)
				w = x.Parent.Left
			}

			if w.Right.Color == Black && w.Left.Color == Black {
				w.Color = Red
				x = x.Parent
			} else {
				if w.Left.Color == Black {
					w.Right.Color = Black
					w.Color = Red
					t.RotateLeft(w)
					w = x.Parent.Left
				}

				w.Color = x.Parent.Color
				x.Parent.Color = Black
				w.Left.Color = Black
				t.RotateRight(x.Parent)
				x = t.Root
			}
		}
	}

	x.Color = Black
}






