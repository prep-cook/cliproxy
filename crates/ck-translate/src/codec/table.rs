use super::{Error, Result};
use ck_stream::{MetadataPermit, Writer};

/// Capacity accounting for position/scalar tables; payload bytes never belong here.
pub(crate) struct Table<T> {
    values: Vec<T>,
    permit: MetadataPermit,
}
impl<T> Table<T> {
    pub fn new(owner: &mut Writer) -> Result<Self> {
        Ok(Self::with_permit(owner.reserve_metadata(0)?))
    }
    /// An empty table charged to an existing permit.
    pub fn with_permit(permit: MetadataPermit) -> Self {
        Self {
            values: Vec::new(),
            permit,
        }
    }
    /// Frees the values and returns the permit with its charge released.
    pub fn into_permit(mut self) -> Result<MetadataPermit> {
        self.values = Vec::new();
        self.permit.resize(0)?;
        Ok(self.permit)
    }
    pub fn push(&mut self, value: T) -> Result<usize> {
        self.permit.check_live()?;
        if self.values.len() == self.values.capacity() {
            let capacity = self.values.capacity().max(4).checked_mul(2).ok_or(Error::Limit)?;
            self.permit
                .resize(capacity.checked_mul(std::mem::size_of::<T>()).ok_or(Error::Limit)?)?;
            self.values
                .try_reserve_exact(capacity - self.values.len())
                .map_err(|_| Error::Limit)?;
            self.permit.resize(self.values.capacity() * std::mem::size_of::<T>())?;
        }
        let index = self.values.len();
        self.values.push(value);
        Ok(index)
    }
    /// Removes one value in O(1), moving the last value into its place; capacity stays charged.
    pub fn swap_remove(&mut self, index: usize) -> T {
        self.values.swap_remove(index)
    }
}
impl<T> std::ops::Deref for Table<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.values
    }
}
impl<T> std::ops::DerefMut for Table<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.values
    }
}
