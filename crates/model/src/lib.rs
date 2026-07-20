use std::collections::HashMap;
pub type ObjectId = [u8; 32];
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Model {
    pub objects: HashMap<ObjectId, Vec<u8>>,
    pub root: Option<ObjectId>,
}
impl Model {
    pub fn put(&mut self, id: ObjectId, bytes: Vec<u8>) {
        self.objects.entry(id).or_insert(bytes);
    }
}
