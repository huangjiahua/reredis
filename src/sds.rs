type Sds = String;

pub trait SdsAction {
    fn sds_empty() -> Sds;
    fn sds_len(&self) -> usize;
    fn sds_avail(&self) -> usize;
}

impl SdsAction for Sds {
    fn sds_empty() -> Sds {
        Sds::from("")
    }
    fn sds_len(&self) -> usize {
        self.len()
    }

    fn sds_avail(&self) -> usize {
        self.capacity() - self.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_empty_sds() {
        let s = Sds::sds_empty();
        assert_eq!(s, "");
    }

    #[test]
    fn get_len_of_sds() {
        let s = Sds::from("hello");
        assert_eq!(s.sds_len(), 5);
        let s = Sds::sds_empty();
        assert_eq!(s.sds_len(), 0);
    }

    #[test]
    fn get_available_space() {
        let s = Sds::sds_empty();
        assert!(s.sds_avail() >= 0);
        let s = Sds::from("hello");
        assert!(s.sds_avail() >= 0);
    }
}