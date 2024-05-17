use std::fmt;
use std::fmt::{Debug, Formatter};
use std::mem::replace;
use std::ops::{Index, IndexMut};
use std::slice::{Iter, IterMut};
use std::iter::Flatten;
use std::vec::IntoIter;

pub struct VecMap<V> {
    n: usize,
    v: Vec<Option<V>>,
}

impl<V> Default for VecMap<V> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<V> VecMap<V> {
    pub fn new() -> Self {
        VecMap { n: 0, v: vec![] }
    }
    pub fn with_capacity(capacity: usize) -> Self {
        VecMap {
            n: 0,
            v: Vec::with_capacity(capacity),
        }
    }
    #[inline]
    pub fn capacity(&self) -> usize {
        self.v.capacity()
    }
    pub fn reserve_len(&mut self, len: usize) {
        let cur_len = self.v.len();
        if len >= cur_len {
            self.v.reserve(len - cur_len);
        }
    }
    pub fn reserve_len_exact(&mut self, len: usize) {
        let cur_len = self.v.len();
        if len >= cur_len {
            self.v.reserve_exact(len - cur_len);
        }
    }
    pub fn shrink_to_fit(&mut self) {
        // strip off trailing `None`s
        if let Some(idx) = self.v.iter().rposition(Option::is_some) {
            self.v.truncate(idx + 1);
        } else {
            self.v.clear();
        }

        self.v.shrink_to_fit()
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }
    pub fn clear(&mut self) {
        self.n = 0;
        self.v.clear()
    }

    #[inline]
    pub fn get(&self, key: usize) -> Option<&V> {
        if key < self.v.len() {
            self.v[key].as_ref()
        } else {
            None
        }
    }

    pub fn get_many_mut<const N: usize>(&mut self, keys: [usize; N]) -> Option<[&mut V; N]> {
        self.v
            .get_many_mut(keys)
            .ok()
            .and_then(|x| x.try_map(|x| x.as_mut()))
    }

    pub unsafe fn get_many_unchecked_mut<const N: usize>(
        &mut self,
        keys: [usize; N],
    ) -> [&mut V; N] {
        self.v
            .get_many_unchecked_mut(keys)
            .map(|x| x.as_mut().unwrap_unchecked())
    }

    #[inline]
    pub unsafe fn get_unchecked(&self, key: usize) -> &V {
        self.v.get_unchecked(key).as_ref().unwrap_unchecked()
    }

    #[inline]
    pub fn contains_key(&self, key: usize) -> bool {
        self.get(key).is_some()
    }
    #[inline]
    pub fn get_mut(&mut self, key: usize) -> Option<&mut V> {
        if key < self.v.len() {
            self.v[key].as_mut()
        } else {
            None
        }
    }
    #[inline]
    pub fn get_or_insert_with(&mut self, key: usize, value: impl FnOnce() -> V) -> &mut V{
        let len = self.v.len();
        if len <= key {
            self.v.extend((0..key - len + 1).map(|_| None));
            unsafe{
                self.n += 1;
                return  self.v.get_unchecked_mut(key).insert(value())
            }
        }
        unsafe { self.v.get_unchecked_mut(key).get_or_insert_with(||{
            self.n += 1;
            value()
        }) }

    }
    #[inline]
    pub fn get_or_insert(&mut self, key: usize, value: V) -> &mut V{
        let len = self.v.len();
        if len <= key {
            self.v.extend((0..key - len + 1).map(|_| None));
            unsafe{
                self.n += 1;
                return  self.v.get_unchecked_mut(key).insert(value)
            }
        }
        unsafe { self.v.get_unchecked_mut(key).get_or_insert_with(||{
            self.n += 1;
            value
        }) }

    }

    #[inline]
    pub unsafe fn get_unchecked_mut(&mut self, key: usize) -> &mut V {
        self.v.get_unchecked_mut(key).as_mut().unwrap_unchecked()
    }

    pub fn insert(&mut self, key: usize, value: V) -> Option<V> {
        let len = self.v.len();
        if len <= key {
            self.v.extend((0..key - len + 1).map(|_| None));
        }
        let was = replace(&mut self.v[key], Some(value));
        if was.is_none() {
            self.n += 1;
        }
        was
    }

    pub fn remove(&mut self, key: usize) -> Option<V> {
        if key >= self.v.len() {
            return None;
        }
        let result = &mut self.v[key];
        let was = result.take();
        if was.is_some() {
            self.n -= 1;
        }
        was
    }

    pub unsafe fn remove_unchecked(&mut self, key: usize) -> V {
        let result = self.v.get_unchecked_mut(key);
        let was = result.take().unwrap_unchecked();
        self.n -= 1;
        was
    }

    pub fn iter(&self) -> impl Iterator<Item = &V>{
        self.into_iter()
    }

    pub fn key_value(&self) -> impl Iterator<Item = (usize, &V)>{
        self.v.iter().enumerate().filter_map(|(idx,val)| val.as_ref().map(|val| (idx, val)))
    }

    pub fn into_key_value(self) -> impl Iterator<Item = (usize, V)>{
        self.v.into_iter().enumerate().filter_map(|(idx,val)| val.map(|val| (idx, val)))
    }
    pub fn rev_key_value(&self) -> impl Iterator<Item = (usize, &V)>{
        self.v.iter().rev().enumerate().filter_map(|(idx,val)| val.as_ref().map(|val| (idx, val)))
    }
}



impl<V> IntoIterator for VecMap<V> {
    type Item = V;
    type IntoIter = Flatten<IntoIter<Option<V>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.v.into_iter().flatten()
    }
}

impl<'a, V> IntoIterator for &'a mut VecMap<V> {
    type Item = &'a mut V;
    type IntoIter = Flatten<IterMut<'a, Option<V>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.v.iter_mut().flatten()
    }
}

impl<'a, V> IntoIterator for &'a VecMap<V> {
    type Item = &'a V;
    type IntoIter = Flatten<Iter<'a, Option<V>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.v.iter().flatten()
    }
}


impl<V: Debug> Debug for VecMap<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.v.iter().enumerate().filter_map(|x| {
                if x.1.is_some() {
                    Some((x.0, x.1.as_ref().unwrap()))
                } else {
                    None
                }
            }))
            .finish()
    }
}

impl<V: Clone> Clone for VecMap<V> {
    #[inline]
    fn clone(&self) -> Self {
        VecMap {
            n: self.n,
            v: self.v.clone(),
        }
    }

    #[inline]
    fn clone_from(&mut self, source: &Self) {
        self.v.clone_from(&source.v);
        self.n = source.n;
    }
}

impl<V> Index<usize> for VecMap<V> {
    type Output = V;

    #[inline]
    fn index(&self, i: usize) -> &V {
        self.get(i).expect("key not present")
    }
}

impl<'a, V> Index<&'a usize> for VecMap<V> {
    type Output = V;

    #[inline]
    fn index(&self, i: &usize) -> &V {
        self.get(*i).expect("key not present")
    }
}

impl<V> IndexMut<usize> for VecMap<V> {
    #[inline]
    fn index_mut(&mut self, i: usize) -> &mut V {
        self.get_mut(i).expect("key not present")
    }
}

impl<'a, V> IndexMut<&'a usize> for VecMap<V> {
    #[inline]
    fn index_mut(&mut self, i: &usize) -> &mut V {
        self.get_mut(*i).expect("key not present")
    }
}

impl<V> FromIterator<(usize, V)> for VecMap<V>{
    fn from_iter<T: IntoIterator<Item=(usize, V)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let size = iter.size_hint().0;
        let mut vec_map = VecMap::with_capacity(size);
        for (key,value) in iter{
            vec_map.insert(key, value);
        }

        vec_map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_many_unchecked() {
        let mut map = VecMap::new();
        map.insert(1,10);
        map.insert(2,15);
        map.insert(5,20);
        map.insert(2,6);
        println!("{:?}", map);

        let many = unsafe { map.get_many_unchecked_mut([1, 5,2]) };
        println!("{:?}", many);

    }
}