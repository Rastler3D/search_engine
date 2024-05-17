use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut, Range, RangeBounds};
use bitset_core::BitSet as b;

#[derive(Eq, PartialEq, Hash)]
pub struct BitSet<STORAGE = [u64;2]>{
    len: usize,
    inner: STORAGE
}

impl BitSet<Vec<u64>>{

    #[inline]
    pub fn new_vec() -> Self {
        Self{
            len: 0,
            inner: vec![]
        }
    }

    #[inline]
    pub fn init(value: bool, len: usize) -> BitSet<Vec<u64>>{
        let last = 2u64.pow((len % 64) as u32) - 1;
        let size = len / 64 + 1;
        let (init_val,len) = if value{
            (u64::MAX,len)
        } else { (0,0) };
        let mut vec = vec![init_val; size];
        vec.last_mut().map(|x| *x = last);
        BitSet{
            inner: vec,
            len,
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self{
       BitSet{
           len: 0,
           inner: Vec::with_capacity(capacity)
       }
    }
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[inline]
    pub fn storage(&self) -> &[u64]  {
        &self.inner
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        let len = self.inner.len() * 64;

        (0..len).filter_map(|i|if self.inner.bit_test(i) { Some(i) } else { None })
    }


    #[inline]
    pub fn clear(&mut self) {

        self.inner.clear();
        self.len = 0;
    }
    #[inline]
    pub fn contains(&self, value: usize) -> bool {
        let idx = value / 64;
        idx < self.inner.len() && self.inner.bit_test(value)
    }

    #[inline]
    pub fn insert(&mut self, value: usize) -> bool {
        let idx = (value / 64);
        let len = self.inner.len();
        if len <= idx {
            self.inner.extend((0..idx - len + 1 ).map(|_| 0));
        }

        if !self.inner.bit_test(value){
            self.inner.bit_set(value);
            self.len +=1;
            true
        } else { false }
    }

    #[inline]
    pub fn insert_range(&mut self, range: Range<usize>){
        for i in range{
            self.insert(i);
        }
    }
    #[inline]
    pub fn remove(&mut self, value: (usize)) -> bool {
        let idx = value / 64;
       if idx < self.inner.len() && self.inner.bit_test(value){
           self.inner.bit_reset(value);
           self.len -=1;
           true
       } else { false }
    }
}

impl<const N:usize> BitSet<[u64;N]>{
    const SIZE: usize = 64 * N;

    #[inline]
    pub const fn new() -> Self {
        Self{
            len: 0,
            inner: [0;N]
        }
    }
    #[inline]
    pub fn init(value: bool, len: usize) -> BitSet<[u64;N]>{
        if len > Self::SIZE { panic!("Len must be less then 128") }

        let last = 2u64.pow((len % 64) as u32) - 1;
        let max_idx = len / 64;
        let (init_val,len) = if value{
            (u64::MAX,len)
        } else { (0,0) };
        let mut arr = [0; N];
        for idx in 0..max_idx{
            arr[idx] = init_val;
        }
        arr[max_idx] = last;
        BitSet{
            inner: arr,
            len,
        }
    }
    #[inline]
    pub fn storage(&self) -> &[u64]  {
        &self.inner
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        (0..Self::SIZE).filter_map(|i| if self.inner.bit_test(i) { Some(i) } else { None })
    }

    #[inline]
    pub fn clear(&mut self) {

        self.inner = [0;N];
        self.len = 0;
    }
    #[inline]
    pub fn contains(&self, value: usize) -> bool {
        self.inner.bit_test(value)
    }
    #[inline]
    pub fn insert(&mut self, value: usize) -> bool {
        if value >= Self::SIZE{
            return false
        }
        if !self.inner.bit_test(value){
            self.inner.bit_set(value);
            self.len +=1;
            true
        } else { false }
    }

    #[inline]
    pub fn insert_range(&mut self, range: Range<usize>){
        for i in range{
            self.insert(i);
        }
    }
    #[inline]
    pub fn remove(&mut self, value: usize) -> bool {
        if value >= Self::SIZE{
            return false
        }
        if self.inner.bit_test(value){
            self.inner.bit_reset(value);
            self.len -=1;
            true
        } else { false }
    }

    #[inline]
    pub fn exclude(&self, value: usize) -> Self{
        let mut clone = self.clone();
        clone.remove(value);
        clone
    }
}

impl <T: Default> BitSet<T> {

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl BitSet<Vec<u64>> {
    #[inline]
    pub fn is_disjoint(&self, other: &BitSet) -> bool {
        (*self.inner).bit_disjoint(&other.inner)
    }

    #[inline]
    pub fn is_subset(&self, other: &BitSet) -> bool {
        (*self.inner).bit_subset(&other.inner)
    }
    #[inline]
    pub fn is_superset(&self, other: &BitSet) -> bool {
        (*self.inner).bit_superset(&other.inner)
    }

    #[inline]
    pub fn intersection(&mut self, other: &BitSet) -> &mut Self {
        let other_len = other.inner.len();
        let len = self.inner.len();
        if len <= other_len {
            self.inner.extend((0..other_len - len + 1 ).map(|_| 0));
        }
        (*self.inner).bit_and(&other.inner);
        self.len = self.inner.bit_count();
        self
    }

    #[inline]
    pub fn union(&mut self, other: &BitSet) -> &mut Self{
        let other_len = other.inner.len();
        let len = self.inner.len();
        if len <= other_len {
            self.inner.extend((0..other_len - len + 1 ).map(|_| 0));
        }
        (*self.inner).bit_or(&other.inner);
        self.len = self.inner.bit_count();
        self
    }

    #[inline]
    pub fn difference(&mut self, other: &BitSet) -> &mut Self{
        let other_len = other.inner.len();
        let len = self.inner.len();
        if len <= other_len {
            self.inner.extend((0..other_len - len + 1 ).map(|_| 0));
        }
        (*self.inner).bit_andnot(&other.inner);
        self.len = self.inner.bit_count();
        self
    }

}

impl BitSet<[u64;2]> {
    #[inline]
    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.inner.bit_disjoint(&other.inner)
    }

    #[inline]
    pub fn is_subset(&self, other: &Self) -> bool {
        self.inner.bit_subset(&other.inner)
    }
    #[inline]
    pub fn is_superset(&self, other: &Self) -> bool {
        self.inner.bit_superset(&other.inner)
    }

    #[inline]
    pub fn intersection(&mut self, other: &Self) -> &mut Self {
        self.inner.bit_and(&other.inner);
        self.len = self.inner.bit_count();
        self
    }

    #[inline]
    pub fn union(&mut self, other: &Self) -> &mut Self{
        self.inner.bit_or(&other.inner);
        self.len = self.inner.bit_count();
        self
    }

    #[inline]
    pub fn difference(&mut self, other: &Self) -> &mut Self{
        self.inner.bit_andnot(&other.inner);
        self.len = self.inner.bit_count();
        self
    }

}

impl Debug for BitSet<Vec<u64>>{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_set()
            .entries(self.iter())
            .finish()
    }
}

impl<const N: usize> Debug for BitSet<[u64;N]>{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_set()
            .entries(self.iter())
            .finish()
    }
}

impl<T:Clone> Clone for BitSet<T> {
    fn clone(&self) -> Self {
        Self {
            len: self.len,
            inner: self.inner.clone(),
        }
    }
}

impl<const N: usize> Copy for BitSet<[u64;N]>{

}

impl<T: Default> Default for BitSet<T> {
    #[inline]
    fn default() -> Self {
        Self {
            inner: Default::default(),
            len: 0
        }
    }
}

impl<T: Default> FromIterator<usize> for BitSet<T>
where BitSet<T>: Extend<usize>{
    #[inline]
    fn from_iter<I: IntoIterator<Item = usize>>(iter: I) -> Self {
        let mut ret = Self::default();
        ret.extend(iter);
        ret
    }
}

impl<const N: usize> Extend<usize> for BitSet<[u64;N]> {
    #[inline]
    fn extend<I: IntoIterator<Item = usize>>(&mut self, iter: I) {
        for i in iter {
            self.insert(i);
        }
    }
}

impl<const N: usize> From<[u64;N]> for BitSet<[u64;N]> {
    fn from(value: [u64; N]) -> Self {
        Self{
            len: value.bit_count(),
            inner: value
        }
    }
}

impl Extend<usize> for BitSet<Vec<u64>> {
    #[inline]
    fn extend<I: IntoIterator<Item = usize>>(&mut self, iter: I) {
        for i in iter {
            self.insert(i);
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_set_len() {
        let mut set:BitSet<[u64;4]> = BitSet::new();
        assert_eq!(set.len, 0);
        for i in 0..100{
            set.insert(i);
        }
        assert_eq!(set.len, 100);
    }

    #[test]
    fn bit_set_insert() {
        let mut set:BitSet<[u64;4]> = BitSet::new();
        for i in 0..100{
            set.insert(i);
        }
        assert_eq!(set.contains(10), true);
        assert_eq!(set.len, 100);
        assert_eq!(set.insert(15), false);
        assert_ne!(set.len, 101);
    }

    #[test]
    fn bit_set_remove() {
        let mut set:BitSet<[u64;4]> = BitSet::new();
        for i in 0..100{
            set.insert(i);
        }
        assert_eq!(set.remove(10),true);
        assert_eq!(set.remove(101),false);
        assert_eq!(set.contains(10), false);
        assert_eq!(set.len, 99);
    }

    #[test]
    fn bit_set_clear() {
        let mut set:BitSet<[u64;4]> = BitSet::new();
        for i in 0..100{
            set.insert(i);
        }
        set.clear();
        assert_eq!(set.remove(5),false);
        assert_eq!(set.contains(10), false);
        assert_eq!(set.len,0);
    }

    #[test]
    fn bit_set_from_iter() {
        let iter = 0..100;
        let mut set:BitSet<[u64;4]> = BitSet::from_iter(iter);

        assert_eq!(set.insert(5),false);
        assert_eq!(set.contains(10), true);
        assert_eq!(set.len,100);
    }

    #[test]
    fn bit_set_into_iter() {
        let iter = 0..100;
        //let iter2 = 200..300;
        let mut set:BitSet<[u64;4]> = BitSet::from_iter(iter);
        //set.extend(iter2);
        assert_eq!(set.len,100);
        assert_eq!(set.iter().eq((0..100)), true);
    }

    #[test]
    fn bit_set_init() {
        let mut set:BitSet<[u64;4]> = BitSet::<[u64;4]>::init(true,128);

        assert_eq!(set.len,128);

        assert_eq!(set.iter().eq(0..128), true);
    }
}