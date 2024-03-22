pub trait StrUtil {
    fn sub_str(&self, start: usize, end: usize) -> String;
    fn extract_middle(&self, prefix: &str, suffix: &str) -> String;
    fn extract_after_last(&self, sub_str: &str) -> String;
    fn trim_left_slash(&self) -> &str;

    fn trim_right_slash(& self) -> & str;
}

impl StrUtil for &str {
    fn sub_str(&self, start: usize, end: usize) -> String {
        String::from(&self[start .. end])
    }
    fn extract_middle(&self, prefix: &str, suffix: &str) -> String {
        String::from(&self[prefix.len() .. self.len() - suffix.len()])
    }
    fn extract_after_last(&self, sub_str: &str) -> String {
        let index = self.rfind(sub_str).unwrap();
        String::from(&self[index + sub_str.len() .. self.len()])
    }
    fn trim_left_slash(& self) -> & str {
        fn trim_left_slash_inner(s: &str) -> &str {
            if s.chars().next() == Some('/') {
                trim_left_slash_inner(&s['/'.len_utf8() ..])
            } else {
                s
            }
        }
        trim_left_slash_inner(self)
    }

    fn trim_right_slash(& self) -> & str {
        let slashes_cnt = self.chars().into_iter().rev().take_while(|c| *c == '/').count();
        &self[0 .. self.len() - ('/'.len_utf8() * slashes_cnt)]
    }
}

#[test]
fn test_sub_str() {
    assert_eq!("quick brown fox".sub_str(6, 11), "brown".to_string());
}

#[test]
fn test_extract_middle() {
    assert_eq!("/home/user/123456.avro".extract_middle("/home/user/", ".avro"), "123456");
}

#[test]
fn test_extract_after_last() {
    assert_eq!("/home/user/123456.avro".extract_after_last("/"), "123456.avro");
}

#[test]
fn test_trim_left_slash() {
    assert_eq!("/aaa/bbb/".trim_left_slash(), "aaa/bbb/");
}

#[test]
fn test_trim_right_slash() {
    assert_eq!("/aaa/bbb/".trim_right_slash(), "/aaa/bbb");
    assert_eq!("/aaa/bbb///".trim_right_slash(), "/aaa/bbb");
    assert_eq!("eai3_status_spark_1.2.0/".to_string().as_str().trim_right_slash(), "eai3_status_spark_1.2.0");
}