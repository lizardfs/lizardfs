/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "config.h"

#include <stdlib.h>

#include "mfscommon/massert.h"
#include "itree.h"

typedef struct _itnode {
	uint32_t from,to;
	uint32_t id;
	struct _itnode *left,*right;
} itnode;

static inline void itree_free(itnode *n) {
	if (n) {
		itree_free(n->left);
		itree_free(n->right);
		free(n);
	}
}

static inline void itree_remove(itnode **p) {
	itnode *n = *p;
	itnode *nit,**nptr;
	uint32_t l,r;
	if (n->left==NULL) {
		*p = n->right;
		free(n);
	} else if (n->right==NULL) {
		*p = n->left;
		free(n);
	} else {
		l = r = 0;
		for (nit=n->left->right ; nit ; nit=nit->right) {
			l++;
		}
		for (nit=n->right->left ; nit ; nit=nit->left) {
			r++;
		}
		if (l==r) {
			l+=((n->from)^(n->to))&1;
		}
		if (r>l) {
			nptr = &(n->right);
			while ((nit=*nptr) && nit->left) {
				nptr = &(nit->left);
			}
			*nptr = nit->right;
		} else {
			nptr = &(n->left);
			while ((nit=*nptr) && nit->right) {
				nptr = &(nit->right);
			}
			*nptr = nit->left;
		}
		nit->left = n->left;
		nit->right = n->right;
		*p = nit;
	}
}

static inline void itree_delete(itnode **p,uint32_t f,uint32_t t);
static inline void itree_add(itnode **p,uint32_t f,uint32_t t,uint32_t id);

static inline void itree_delete(itnode **p,uint32_t f,uint32_t t) {
	itnode *n = *p;
	if (n) {
		if (t<n->from) {
			itree_delete(&(n->left),f,t);
		} else if (f>n->to) {
			itree_delete(&(n->right),f,t);
		} else if (f<=n->from && t>=n->to) {
			if (f<n->from) {
				itree_delete(&(n->left),f,n->from-1);
			}
			if (t>n->to) {
				itree_delete(&(n->right),n->to+1,t);
			}
			itree_remove(p);
		} else if (f>=n->from && t<=n->to) {
			if (f==n->from) {
				n->from = t+1;
			} else if (t==n->to) {
				n->to = f-1;
			} else {
				if ((t^f)&1) {
					itree_add(&(n->right),t+1,n->to,n->id);
					n->to = f-1;
				} else {
					itree_add(&(n->left),n->from,f-1,n->id);
					n->from = t+1;
				}
			}
		} else if (f<n->from) {
			n->from = t+1;
			itree_delete(&(n->left),f,t);
		} else if (t>n->to) {
			n->to = f-1;
			itree_delete(&(n->right),f,t);
		}
	}
}

static inline void itree_add(itnode **p,uint32_t f,uint32_t t,uint32_t id) {
	itnode *n = *p;
	if (n) {
		if (t<n->from) {
			itree_add(&(n->left),f,t,id);
		} else if (f>n->to) {
			itree_add(&(n->right),f,t,id);
		} else if (f<=n->from && t>=n->to) {
			if (f<n->from) {
				itree_delete(&(n->left),f,n->from-1);
			}
			if (t>n->to) {
				itree_delete(&(n->right),n->to+1,t);
			}
			n->from = f;
			n->to = t;
			n->id = id;
		} else if (f>=n->from && t<=n->to) {
			if (f>n->from) {
				itree_add(&(n->left),n->from,f-1,n->id);
			}
			if (t<n->to) {
				itree_add(&(n->right),t+1,n->to,n->id);
			}
			n->from = f;
			n->to = t;
			n->id = id;
		} else if (f<n->from) {
			n->from = t+1;
			itree_add(&(n->left),f,t,id);
		} else if (t>n->to) {
			n->to = f-1;
			itree_add(&(n->right),f,t,id);
		}
	} else {
		*p = n = (itnode*) malloc(sizeof(itnode));
		passert(n);
		n->from = f;
		n->to = t;
		n->id = id;
		n->left = NULL;
		n->right = NULL;
	}
}

itnode** itree_tolist(itnode *n,itnode **tail) {
	if (n) {
		tail = itree_tolist(n->left,tail);
		n->left = NULL;
		*tail = n;
		tail = itree_tolist(n->right,&(n->left));
		n->right = NULL;
	}
	return tail;
}

void itree_simplify(itnode *n) {
	itnode *f;
	while (n && n->left) {
		if (n->id == n->left->id && n->to+1 == n->left->from) {
			n->to = n->left->to;
			f = n->left;
			n->left = n->left->left;
			free(f);
		} else {
			n = n->left;
		}
	}
}

void itree_totree(itnode *l,itnode **p) {
	itnode **m,*i;
	if (l) {
		i = l;
		m = &l;
		while (i && i->left) {
			m = &((*m)->left);
			i = i->left->left;
		}
		*p = i = *m;
		(*m) = NULL;
		itree_totree(i->left,&(i->right));
		itree_totree(l,&(i->left));
	} else {
		*p = NULL;
	}
}

/* interface */

/* very simple square-time tree rebalance */
/* in the future whole tree should be reimplemented using, RB-tree, AVL or splay */

void* itree_rebalance(void *o) {
	itnode *head;
	itnode *root = (itnode*)o;

	head = NULL;
	itree_tolist(root,&head);
	itree_simplify(head);
	root = NULL;
	itree_totree(head,&root);
	return (void*)root;
}

void* itree_add_interval(void *o,uint32_t f,uint32_t t,uint32_t id) {
	itnode *root = (itnode*)o;
	if (id==0) {
		if (t<f) {
			itree_delete(&root,t,f);
		} else {
			itree_delete(&root,f,t);
		}
	} else {
		if (t<f) {
			itree_add(&root,t,f,id);
		} else {
			itree_add(&root,f,t,id);
		}
	}
	return root;
}

uint32_t itree_find(void *o,uint32_t v) {
	itnode *n;

	for (n = (itnode*)o ; n ; n = (v<n->from)?n->left:n->right) {
		if (v>=n->from && v<=n->to) {
			return n->id;
		}
	}
	return 0;

}

void itree_freeall(void *o) {
	itree_free((itnode*)o);
}
