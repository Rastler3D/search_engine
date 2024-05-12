#[derive(Clone)]
pub struct TypoConfig{
    pub max_typos: u32,
    pub word_len_one_typo: u32,
    pub word_len_two_typo: u32,
}

impl TypoConfig {
    pub fn allowed_typos(&self, word: &str) -> u8{
        let len = word.chars().count() as u32;
        if len < self.word_len_one_typo || self.max_typos == 0{
            0
        } else if len < self.word_len_two_typo {
            1
        } else {
            2
        }
    }
}