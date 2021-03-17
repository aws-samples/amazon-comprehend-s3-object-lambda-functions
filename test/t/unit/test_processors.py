import os
import timeit
from random import shuffle
from unittest import TestCase

from constants import REPLACE_WITH_PII_ENTITY_TYPE
from data_object import Document, RedactionConfig
from exceptions import InvalidConfigurationException
from processors import Redactor, Segmenter

this_module_path = os.path.dirname(__file__)


class ProcessorsTest(TestCase):
    def test_segmenter_basic_text(self):
        segmentor = Segmenter(50, overlap_tokens=3)
        original_text = "Barack Hussein Obama II is an American politician and attorney who served as the " \
                        "44th president of the United States from 2009 to 2017."
        segments = segmentor.segment(original_text)
        expected_segments = [
            "Barack Hussein Obama II is an American politician ",
            "an American politician and attorney who served as ",
            "who served as the 44th president of the United ",
            "of the United States from 2009 to 2017."]
        for expected_segment, actual_segment in zip(expected_segments, segments):
            assert expected_segment == actual_segment.text
        shuffle(segments)
        assert segmentor.de_segment(segments).text == original_text


    def test_segmenter_no_segmentation_needed(self):
        segmentor = Segmenter(5000, overlap_tokens=3)
        original_text = "Barack Hussein Obama II is an American politician and attorney who served as the " \
                        "44th president of the United States from 2009 to 2017."
        segments = segmentor.segment(original_text)
        assert len(segments) == 1
        assert segments[0].text == original_text
        assert segmentor.de_segment(segments).text == original_text

    def test_segmenter_max_chars_limit(self):
        segmentor = Segmenter(50, overlap_tokens=3, max_overlapping_chars=20)
        original_text = "BarackHusseinObamaIIisanAmerican politicianandattorneywhoservedasthe " \
                        "44th president of the United Statesfrom2009to2017."
        segments = segmentor.segment(original_text)
        expected_segments = [
            "BarackHusseinObamaIIisanAmerican ",
            "nObamaIIisanAmerican politician",
            "nAmerican politicianandattorneywhoservedasthe ",
            "torneywhoservedasthe 44th president of the United ",
            "of the United Statesfrom2009to2017.", ]
        for expected_segment, actual_segment in zip(expected_segments, segments):
            assert expected_segment == actual_segment.text
        shuffle(segments)
        assert segmentor.de_segment(segments).text == original_text

    def test_segmenter_unicode_chars(self):
        segmentor = Segmenter(100, overlap_tokens=3)
        original_text = "ʕ•́ᴥ•̀ʔっ♡ Emoticons 😜 ʕ•́ᴥ•̀ʔっ♡ Emoticons 😜 ᗷᙓ ò¥¥¥¥¥¥¥ᗢᖇᓮᘐᓰﬡᗩᒪ ℬ℮ ¢◎øł Bᴇ ʏᴏᴜʀsᴇʟғ विकिपीडिया सभी विषयों पर प्रामाणिक और उपयोग, " \
                        "परिवर्तन व पुनर्वितरण के लिए स्वतन्त्र ज्ञानकोश बनाने hànbǎobāo, hànbǎo 汉堡包/漢堡包, 汉堡/漢堡 – hamburger"
        segments = segmentor.segment(original_text)
        expected_segments = [
            "ʕ•́ᴥ•̀ʔっ♡ Emoticons 😜 ʕ•́ᴥ•̀ʔっ♡ Emoticons 😜 ᗷᙓ ",
            "Emoticons 😜 ᗷᙓ ò¥¥¥¥¥¥¥ᗢᖇᓮᘐᓰﬡᗩᒪ ℬ℮ ¢◎øł Bᴇ ",
            "ℬ℮ ¢◎øł Bᴇ ʏᴏᴜʀsᴇʟғ विकिपीडिया सभी ",
            "ʏᴏᴜʀsᴇʟғ विकिपीडिया सभी विषयों पर ",
            "सभी विषयों पर प्रामाणिक और उपयोग, ",
            "प्रामाणिक और उपयोग, परिवर्तन व ",
            "उपयोग, परिवर्तन व पुनर्वितरण के लिए ",
            "पुनर्वितरण के लिए स्वतन्त्र ",
            "के लिए स्वतन्त्र ज्ञानकोश बनाने hànbǎobāo, ",
            "ज्ञानकोश बनाने hànbǎobāo, hànbǎo 汉堡包/漢堡包, 汉堡/漢堡 ",
            "hànbǎo 汉堡包/漢堡包, 汉堡/漢堡 – hamburger"]
        assert len(expected_segments) == len(segments)
        for expected_segment, actual_segment in zip(expected_segments, segments):
            assert expected_segment == actual_segment.text
        assert segmentor.de_segment(segments).text == original_text

    def test_desegment_overlapping_results(self):
        segments = [
            Document(text="Some Random SSN Some Random email-id Some Random name and address and some credit card number", char_offset=0,
                     pii_classification={'SSN': 0.234, 'EMAIL': 0.765, 'NAME': 0.124, 'ADDRESS': 0.976},
                     pii_entities=[{'Score': 0.234, 'Type': 'SSN', 'BeginOffset': 12, 'EndOffset': 36},
                                   {'Score': 0.765, 'Type': 'EMAIL', 'BeginOffset': 28, 'EndOffset': 36},
                                   {'Score': 0.534, 'Type': 'NAME', 'BeginOffset': 49, 'EndOffset': 53},
                                   {'Score': 0.234, 'Type': 'ADDRESS', 'BeginOffset': 58, 'EndOffset': 65}]),
            Document(text="Some Random name and address and some credit card number", char_offset=37,
                     pii_classification={'SSN': 0.234, 'EMAIL': 0.765, 'USERNAME': 0.424, 'ADDRESS': 0.976},
                     pii_entities=[{'Score': 0.234, 'Type': 'USERNAME', 'BeginOffset': 12, 'EndOffset': 16},
                                   {'Score': 0.634, 'Type': 'ADDRESS', 'BeginOffset': 17, 'EndOffset': 28},
                                   {'Score': 0.234, 'Type': 'CREDIT_DEBIT_NUMBER', 'BeginOffset': 38, 'EndOffset': 56}])]
        segmentor = Segmenter(5000)
        expected_merged_document = Document(
            text="Some Random SSN Some Random email-id Some Random name and address and some credit card number", char_offset=37,
            pii_classification={'SSN': 0.234, 'EMAIL': 0.765, 'NAME': 0.124, 'USERNAME': 0.424, 'ADDRESS': 0.976},
            pii_entities=[{'Score': 0.234, 'Type': 'SSN', 'BeginOffset': 12, 'EndOffset': 36},
                          {'Score': 0.765, 'Type': 'EMAIL', 'BeginOffset': 28, 'EndOffset': 36},
                          {'Score': 0.534, 'Type': 'NAME', 'BeginOffset': 49, 'EndOffset': 53},
                          {'Score': 0.634, 'Type': 'ADDRESS', 'BeginOffset': 54, 'EndOffset': 65},
                          {'Score': 0.234, 'Type': 'CREDIT_DEBIT_NUMBER', 'BeginOffset': 75, 'EndOffset': 93}])
        actual_merged_doc = segmentor.de_segment(segments)
        assert expected_merged_document.text == actual_merged_doc.text
        assert expected_merged_document.pii_classification == actual_merged_doc.pii_classification
        assert expected_merged_document.pii_entities == actual_merged_doc.pii_entities


    def test_is_overlapping_annotations(self):
        segmentor = Segmenter(5000)
        assert segmentor._is_overlapping_annotations({'Score': 0.634, 'Type': 'ADDRESS', 'BeginOffset': 54, 'EndOffset': 65},
                                                     {'Score': 0.234, 'Type': 'ADDRESS', 'BeginOffset': 58, 'EndOffset': 65}) == 0

    def test_segmenter_scalablity_test(self):
        # 1MB of text should be segmented with around 30 ms latency
        setup = """
import os    
from processors import Segmenter

text=" Hello Zhang Wei. Your AnyCompany Financial Services, LLC credit card account 1111-0000-1111-0000 has a minimum payment of $24.53"
one_mb_text=" "
for i in range(7751):
    one_mb_text += text 
segmenter = Segmenter(overlap_tokens=20, max_doc_size=5000)
        """.format(this_module_path)
        segmentation_time = timeit.timeit("segmenter.segment(one_mb_text)", setup=setup, number=100)
        assert segmentation_time < 15

    def test_redaction_with_no_entities(self):
        text = "Hello Zhang Wei. Your AnyCompany Financial Services, LLC credit card account 1111-0000-1111-0000 has a minimum payment of $24.53"
        redactor = Redactor(RedactionConfig())
        redacted_text = redactor.redact(text, [])
        assert text == redacted_text

    def test_redaction_default_redaction_config(self):
        text = "Hello Zhang Wei. Your AnyCompany Financial Services, LLC credit card account 1111-0000-1111-0000 has a minimum payment of $24.53"
        redactor = Redactor(RedactionConfig())
        redacted_text = redactor.redact(text, [{'Score': 0.234, 'Type': 'NAME', 'BeginOffset': 6, 'EndOffset': 16},
                                               {'Score': 0.765, 'Type': 'CREDIT_DEBIT_NUMBER', 'BeginOffset': 77, 'EndOffset': 96}])
        expected_redaction = "Hello Zhang Wei. Your AnyCompany Financial Services, LLC credit card account ******************* has a minimum payment of $24.53"
        assert expected_redaction == redacted_text

    def test_redaction_with_replace_entity_type(self):
        text = "Hello Zhang Wei. Your AnyCompany Financial Services, LLC credit card account 1111-0000-1111-0000 has a minimum payment of $24.53"
        redactor = Redactor(RedactionConfig(pii_entity_types=['NAME'], mask_mode=REPLACE_WITH_PII_ENTITY_TYPE, confidence_threshold=0.6))
        redacted_text = redactor.redact(text, [{'Score': 0.634, 'Type': 'NAME', 'BeginOffset': 6, 'EndOffset': 15},
                                               {'Score': 0.765, 'Type': 'CREDIT_DEBIT_NUMBER', 'BeginOffset': 77, 'EndOffset': 96}])
        expected_redaction = "Hello [NAME]. Your AnyCompany Financial Services, LLC credit card account 1111-0000-1111-0000 has a minimum payment of $24.53"
        assert expected_redaction == redacted_text

    def test_segmenter_constructor_invalid_args(self):
        try:
            Segmenter(3)
            assert False, "Expected an InvalidConfigurationException"
        except InvalidConfigurationException:
            return
