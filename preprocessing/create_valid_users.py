# __author__ = 'dimitrios'
"""A file that combines all user post count dictionaries to create a master one, and then save the users
that have more than min_posts posts"""
import os
import random

import numpy as np

from util.preprocessing_util import *
from preprocessing.user_category import dict2matrix
from util.io import save_pickle, load_pickle
from preprocessing.config_filenames import get_valid_user_filename, get_all_user_dict_filenames

random.seed(12345)
ABSOLUTE_MIN_POSTS = 20


def get_user_count_dictionaries(params, years=None):
    """Returns all dictionaries, that are in a dictionary from user -> count"""
    dict_filenames = get_all_user_dict_filenames(params, years)
    return map(load_pickle, dict_filenames)


def create_valid_user_set(params, years=None, overwrite=False):
    """Creates a set of valid users, meaning users with more than min_posts posts"""
    filename = get_valid_user_filename(params, years)
    if os.path.exists(filename) and not overwrite:
        return load_pickle(filename, False)

    user_counts = combine_dicts(get_user_count_dictionaries(params, years))
    usernames = get_top_users(params.min_posts, user_counts)
    usernames = remove_bots(usernames, params)

    print '--> Total valid users: %d' % len(usernames)
    save_pickle(filename, usernames)
    return usernames


def get_h_index(counts):
    """Calculates and Returns the h_index of a counts. Counts have to be sorted."""
    h = 0
    for c in counts:
        if c >= h + 1:
            h += 1
        else:
            break
    return h


def lm_valid_users(filename, params, uxs=None, user_names=None, years=None, overwrite=False):
    """Creates the valid users for language modeling, after applying all previous filters + min h_index filter.

    Args:
        filename: language model  valid users filename to be saved.
        params: Parameters of the preprocessing run
        uxs: User by Subreddit count matrix (sparse).
        user_names: dictionary from user id to user_name.
        years: list of all the years we want to take into consideration. If none, it selects all available.
        overwrite: Boolean to define whether to overwrite existing file.
    Returns:
        A set of user names (who all had an h_index larger than that specified in params.
    """

    if os.path.exists(filename) and not overwrite:
        return load_pickle(filename, False)

    if user_names is None:
        user_set = create_valid_user_set(params, years, overwrite)
        user_names = invert_dict(set_to_dict(user_set))

    if uxs is None:
        uxs = data_to_sparse(dict2matrix(params, valid_users=user_set, years=years))

    assert len(user_names) == uxs.shape[0]

    print 'Calculating h-indices...'
    user_h_index = []
    for u in range(uxs.shape[0]):
        counts = sorted(uxs.getrow(u).data, reverse=True)
        user_h_index.append(get_h_index(counts))
    user_h_index = np.array(user_h_index)

    top_users = np.where(user_h_index >= params.h_index_min)[0]
    top_usernames = set([user_names[u] for u in top_users])

    print 'Total Users: %d -> after pruning with at least %d h-index, %d user left' % (
        len(user_names), params.h_index_min, len(top_usernames))

    save_pickle(filename, top_usernames)
    return top_usernames


def remove_bots(usernames):
    valid_usernames = set()

    for u in usernames:
        if u in known_bots:
            continue
        if is_bot_name(u):
            known_bots.add(u)
            continue
        valid_usernames.add(u)

    print '--> Total known bots: %d' % len(known_bots)
    return valid_usernames


def is_bot_name(name):
    """Some sort of arbitrary rules to filter out bot names."""
    if name in known_bots:
        return True
    if name.endswith('_SS'):
        return True

    if name.lower().endswith('bot'):
        return True
    if name.lower().startswith('auto'):
        return True

    bot_strs = ['_bot', '-bot', 'moderator', 'moderation']
    for s in bot_strs:
        if s in name.lower():
            return True
    return False


def get_top_users(min_posts, user_counts):
    usernames = set()
    print '--> %d Total users' % len(user_counts),
    for (k, v) in user_counts.iteritems():
        if v >= min_posts and v >= ABSOLUTE_MIN_POSTS:
            usernames.add(k)
    print 'pruned to %d (more than %d posts)' % (len(usernames), min_posts)
    return usernames


def get_random_users(total_users, user_counts):
    inactive_users = len(filter(lambda x: x[1] < ABSOLUTE_MIN_POSTS, user_counts.iteritems()))
    percentage = float(total_users) / (len(user_counts) - inactive_users)
    usernames = set()
    print '--> Total Users: %d   Inactive (Less than %d): %d. Sampling with %.03f' % (
        len(user_counts), ABSOLUTE_MIN_POSTS, inactive_users, percentage),
    for (k, v) in user_counts.iteritems():
        if v >= ABSOLUTE_MIN_POSTS:
            r = random.random()
            if r < percentage:
                usernames.add(k)
    return usernames


bot_names = ['A858DE45F56D9BC9',
             'AAbot',
             'ADHDbot',
             'ALTcointip',
             'AVR_Modbot',
             'A_random_gif',
             'AltCodeBot',
             'Antiracism_Bot',
             'ApiContraption',
             'AssHatBot',
             'AtheismModBot',
             'AutoInsult',
             'BELITipBot',
             'BadLinguisticsBot',
             'BanishedBot',
             'BeetusBot',
             'BensonTheBot',
             'Bible_Verses_Bot',
             'BlackjackBot',
             'BlockchainBot',
             'Brigade_Bot',
             'Bronze-Bot',
             'CAH_BLACK_BOT',
             'CHART_BOT',
             'CLOSING_PARENTHESIS',
             'CPTModBot',
             'Cakeday-Bot',
             'CalvinBot',
             'CaptionBot',
             'CarterDugSubLinkBot',
             'CasualMetricBot',
             'Chemistry_Bot',
             'ChristianityBot',
             'Codebreakerbreaker',
             'Comment_Codebreaker',
             'ComplimentingBot',
             'CreepierSmileBot',
             'CreepySmileBot',
             'CuteBot6969',
             'DDBotIndia',
             'DNotesTip',
             'DRKTipBot',
             'DefinitelyBot',
             'DeltaBot',
             'Dictionary__Bot',
             'DidSomeoneSayBoobs',
             'DogeLotteryModBot',
             'DogeTipStatsBot',
             'DogeWordCloudBot',
             'DotaCastingBot',
             'Downtotes_Plz',
             'DownvotesMcGoats',
             'DropBox_Bot',
             'EmmaBot',
             'Epic_Face_Bot',
             'EscapistVideoBot',
             'ExmoBot',
             'ExplanationBot',
             'FTFY_Cat6',
             'FTFY_Cat',
             'FedoraTipAutoBot',
             'FelineFacts',
             'Fixes_GrammerNazi_',
             'FriendSafariBot',
             'FriendlyCamelCaseBot',
             'FrontpageWatch',
             'Frown_Bot',
             'GATSBOT',
             'GabenCoinTipBot',
             'GameDealsBot',
             'Gatherer_bot',
             'GeekWhackBot',
             'GiantBombBot',
             'GifAsHTML5',
             'GoneWildResearcher',
             'GooglePlusBot',
             'GotCrypto',
             'GrammerNazi_',
             'GreasyBacon',
             'Grumbler_bot',
             'GunnersGifsBot',
             'GunnitBot',
             'HCE_Replacement_Bot',
             'HScard_display_bot',
             'Handy_Related_Sub',
             'HighResImageFinder',
             'HockeyGT_Bot',
             'HowIsThisBestOf_Bot',
             'IAgreeBot',
             'ICouldntCareLessBot',
             'IS_IT_SOLVED',
             'I_BITCOIN_CATS',
             'I_Say_No_',
             'Insane_Photo_Bot',
             'IsItDownBot',
             'JiffyBot',
             'JotBot',
             'JumpToBot',
             'KSPortBot',
             'KarmaConspiracy_Bot',
             'LazyLinkerBot',
             'LinkFixerBotSnr',
             'Link_Correction_Bot',
             'Link_Demobilizer',
             'Link_Rectifier_Bot',
             'LinkedCommentBot',
             'LocationBot',
             'MAGNIFIER_BOT',
             'Makes_Small_Text_Bot',
             'Meta_Bot',
             'MetatasticBot',
             'MetricPleaseBot',
             'Metric_System_Bot',
             'MontrealBot',
             'MovieGuide',
             'MultiFunctionBot',
             'MumeBot',
             'NASCARThreadBot',
             'NFLVideoBot',
             'NSLbot',
             'Nazeem_Bot',
             'New_Small_Text_Bot',
             'Nidalee_Bot',
             'NightMirrorMoon',
             'NoSleepAutoMod',
             'NoSobStoryBot2',
             'NobodyDoesThis',
             'NotRedditEnough',
             'PHOTO_OF_CAPTAIN_RON',
             'PJRP_Bot',
             'PhoenixBot',
             'PigLatinsYourComment',
             'PlayStoreLinks_Bot',
             'PlaylisterBot',
             'PleaseRespectTables',
             'PloungeMafiaVoteBot',
             'PokemonFlairBot',
             'PoliteBot',
             'PoliticBot',
             'PonyTipBot',
             'PornOverlord',
             'Porygon-Bot',
             'PresidentObama___',
             'ProselytizerBot',
             'PunknRollBot',
             'QUICHE-BOT',
             'RFootballBot',
             'Random-ComplimentBOT',
             'RandomTriviaBot',
             'Rangers_Bot',
             'Readdit_Bot',
             'Reads_Small_Text_Bot',
             'RealtechPostBot',
             'ReddCoinGoldBot',
             'Relevant_News_Bot',
             'RequirementsBot',
             'RfreebandzBOT',
             'RiskyClickBot',
             'SERIAL_JOKE_KILLER',
             'SMCTipBot',
             'SRD_Notifier',
             'SRS_History_Bot',
             'SRScreenshot',
             'SWTOR_Helper_Bot',
             'SakuraiBot_test',
             'SakuraiBot',
             'SatoshiTipBot',
             'ShadowBannedBot',
             'ShibeBot',
             'ShillForMonsanto',
             'Shiny-Bot',
             'ShittyGandhiQuotes',
             'ShittyImageBot',
             'SketchNotSkit',
             'SmallTextReader',
             'Smile_Bot',
             'Somalia_Bot',
             'Some_Bot',
             'StackBot',
             'StarboundBot',
             'StencilTemplateBOT',
             'StreetFightMirrorBot',
             'SuchModBot',
             'SurveyOfRedditBot',
             'TOP_COMMENT_OF_YORE',
             'Text_Reader_Bot',
             'TheSwedishBot',
             'TipMoonBot',
             'TitsOrGTFO_Bot',
             'TweetPoster',
             'Twitch2YouTube',
             'Unhandy_Related_Sub',
             'UnobtaniumTipBot',
             'UrbanDicBot',
             'UselessArithmeticBot',
             'UselessConversionBot',
             'VideoLinkBot',
             'VideopokerBot',
             'VsauceBot',
             'WWE_Network_Bot',
             'WeAppreciateYou',
             'Website_Mirror_Bot',
             'WeeaBot',
             'WhoWouldWinBot',
             'Wiki_Bot',
             'Wiki_FirstPara_bot',
             'WikipediaCitationBot',
             'Wink-Bot',
             'WordCloudBot2',
             'WritingPromptsBot',
             'X_BOT',
             'YT_Bot',
             '_Definition_Bot_',
             '_FallacyBot_',
             '_Rita_',
             '__bot__',
             'albumbot',
             'allinonebot',
             'annoying_yes_bot',
             'asmrspambot',
             'astro-bot',
             'auto-doge',
             'automoderator',
             'autourbanbot',
             'autowikibot',
             'bRMT_Bot',
             'bad_ball_ban_bot',
             'ban_pruner',
             'baseball_gif_bot',
             'beecointipbot',
             'bitcoinpartybot',
             'bitcointip',
             'bitofnewsbot',
             'bocketybot',
             'c5bot',
             'c5bot',
             'cRedditBot',
             'callfloodbot',
             'callibot',
             'canada_goose_tip_bot',
             'changetip',
             'cheesecointipbot',
             'chromabot',
             'classybot',
             'coinflipbot',
             'coinyetipper',
             'colorcodebot',
             'comment_copier_bot',
             'compilebot',
             'conspirobot',
             'creepiersmilebot',
             'cris9696',
             'cruise_bot',
             'd3posterbot',
             'define_bot',
             'demobilizer',
             'dgctipbot',
             'digitipbot',
             'disapprovalbot',
             'dogetipbot',
             'earthtipbot',
             'edmprobot',
             'elMatadero_bot',
             'elwh392',
             'expired_link_bot',
             'fa_mirror',
             'fact_check_bot',
             'faketipbot',
             'fedora_tip_bot',
             'fedoratips',
             'flappytip',
             'flips_title',
             'foreigneducationbot',
             'frytipbot',
             'fsctipbot',
             'gabenizer-bot',
             'gabentipbot',
             'gfy_bot',
             'gfycat-bot-sucksdick',
             'gifster_bot',
             'gives_you_boobies',
             'givesafuckbot',
             'gocougs_bot',
             'godwin_finder',
             'golferbot',
             'gracefulcharitybot',
             'gracefulclaritybot',
             'gregbot',
             'groompbot',
             'gunners_gif_bot',
             'haiku_robot',
             'havoc_bot',
             'hearing-aid_bot',
             'hearing_aid_bot',
             'hearingaid_bot',
             'hit_bot',
             'hockey_gif_bot',
             'howstat',
             'hwsbot',
             'imgurHostBot',
             'imgur_rehosting',
             'imgurtranscriber',
             'imirror_bot',
             'isitupbot',
             'jerkbot-3hunna',
             'keysteal_bot',
             'kittehcointipbot',
             'last_cakeday_bot',
             'linkfixerbot1',
             'linkfixerbot2',
             'linkfixerbot3',
             'loser_detector_bot',
             'luckoftheshibe',
             'makesTextSmall',
             'malen-shutup-bot',
             'matthewrobo',
             'meme_transcriber',
             'memedad-transcriber',
             'misconception_fixer',
             'mma_gif_bot',
             'moderator-bot',
             'nba_gif_bot',
             'new_eden_news_bot',
             'nhl_gif_bot',
             'not_alot_bot',
             'notoverticalvideo',
             'nyantip',
             'okc_rating_bot',
             'pandatipbot',
             'pandatips',
             'potdealer',
             'provides-id',
             'qznc_bot',
             'rSGSpolice',
             'r_PictureGame',
             'raddit-bot',
             'randnumbot',
             'rarchives',
             'readsmalltextbot',
             'redditbots',
             'redditreviewbot',
             'redditreviewbot',
             'reddtipbot',
             'relevantxkcd-bot',
             'request_bot',
             'rhiever-bot',
             'rightsbot',
             'rnfl_robot',
             'roger_bot',
             'rss_feed',
             'rubycointipbot',
             'rule_bot',
             'rusetipbot',
             'sentimentviewbot',
             'serendipitybot',
             'shadowbanbot',
             'slapbot',
             'slickwom-bot',
             'snapshot_bot',
             'soccer_gif_bot',
             'softwareswap_bot',
             'sports_gif_bot',
             'spursgifs_xposterbot',
             'stats-bot',
             'steam_bot',
             'subtext-bot',
             'synonym_flash',
             'tabledresser',
             'techobot',
             'tennis_gif_bot',
             'test_bot0x00',
             'tipmoonbot1',
             'tipmoonbot2',
             'tittietipbot',
             'topcoin_tip',
             'topredditbot',
             'totes_meta_bot',
             'ttumblrbots',
             'unitconvert',
             'valkyribot',
             'versebot',
             'vertcoinbot',
             'vertcointipbot',
             'wheres_the_karma_bot',
             'wooshbot',
             'xkcd_bot',
             'xkcd_number_bot',
             'xkcd_number_bot',
             'xkcd_number_bot',
             'xkcd_transcriber',
             'xkcdcomic_bot',
             'yes_it_is_weird',
             'yourebot',
             'HCE_Replacement_Bot',
             'Kevin_Garnett_Bot',
             'Rangers_Bot',
             'DropBox_Bot',
             'Website_Mirror_Bot',
             'Metric_System_Bot',
             'Fedora-Tip-Bot',
             'Some_Bot',
             'Brigade_Bot',
             'Link_Correction_Bot',
             'Porygon-Bot',
             'KarmaConspiracy_Bot',
             'SWTOR_Helper_Bot',
             'annoying_yes_bot',
             'wtf_content_bot',
             'Insane_Photo_Bot',
             'Antiracism_Bot',
             'qznc_bot',
             'mma_gif_bot',
             'QUICHE-BOT',
             'bRMT_Bot',
             'hockey_gif_bot',
             'nba_gif_bot',
             'gifster_bot',
             'imirror_bot',
             'okc_rating_bot',
             'tennis_gif_bot',
             'nfl_gif_bot',
             'CPTModBot',
             'LocationBot',
             'CreepySmileBot',
             'FriendSafariBot',
             'WritingPromptsBot',
             'CreepierSmileBot',
             'IAgreeBot',
             'Cakeday-Bot',
             'Meta_Bot',
             'HockeyGT_Bot',
             'soccer_gif_bot',
             'gunners_gif_bot',
             'xkcd_number_bot',
             'GWHistoryBot',
             'PokemonFlairBot',
             'ChristianityBot',
             'cRedditBot',
             'StreetFightMirrorBot',
             'FedoraTipAutoBot',
             'UnobtaniumTipBot',
             'astro-bot',
             'TipMoonBot',
             'PlaylisterBot',
             'Wiki_Bot',
             'fedora_tip_bot',
             'GunnersGifsBot',
             'PGN-Bot',
             'GunnitBot',
             'havoc_bot',
             'Relevant_News_Bot',
             'gfy_bot',
             'RealtechPostBot',
             'imgurHostBot',
             'Gatherer_bot',
             'JumpToBot',
             'DeltaBot',
             'Nazeem_Bot',
             'PhoenixBot',
             'AtheismModBot',
             'IsItDownBot',
             'malo_the_bot',
             'RFootballBot',
             'KSPortBot',
             'Makes_Small_Text_Bot',
             'CompileBot',
             'SakuraiBot',
             'asmrspambot',
             'SurveyOfRedditBot',
             'RfreebandzBOT',
             'rule_bot',
             'xkcdcomic_bot',
             'PloungeMafiaVoteBot',
             'PoliticBot',
             'Dickish_Bot_Bot',
             'SuchModBot',
             'MultiFunctionBot',
             'CasualMetricBot',
             'xkcd_bot',
             'VerseBot',
             'BeetusBot',
             'GameDealsBot',
             'BadLinguisticsBot',
             'rhiever-bot',
             'gfycat-bot-sucksdick',
             'chromabot',
             'Readdit_Bot',
             'wooshbot',
             '',
             'disapprovalbot',
             'request_bot',
             'define_bot',
             'dogetipbot',
             'techobot',
             'CaptionBot',
             'rightsbot',
             'colorcodebot',
             'roger_bot',
             'ADHDbot',
             'hearing-aid_bot',
             'WikipediaCitationBot',
             'PonyTipBot',
             'fact_check_bot',
             'rusetipbot',
             'test_bot0x00',
             'classybot',
             'NFLVideoBot',
             'MAGNIFIER_BOT',
             'WordCloudBot2',
             'JotBot',
             'WeeaBot',
             'raddit-bot',
             'comment_copier_bot',
             'coinflipbot',
             'VideoLinkBot',
             'new_eden_news_bot',
             'hwsbot',
             'UrbanDicBot',
             'hearingaid_bot',
             'thankyoubot',
             'GeekWhackBot',
             'ExmoBot',
             'CHART_BOT',
             'tips_bot',
             'GATSBOT',
             'allinonebot',
             'moderator-bot',
             'rnfl_robot',
             'StackBot',
             'GooglePlusBot',
             'hit_bot',
             'randnumbot',
             'CAH_BLACK_BOT',
             'CalvinBot',
             'DogeTipStatsBot',
             'autourbanbot',
             'GabenCoinTipBot',
             '_Definition_Bot_',
             'redditbots',
             'redditreviewbot',
             '__bot__',
             'autowikibot',
             'golferbot',
             'topredditbot',
             'c5bot',
             'jerkbot-3hunna',
             'gracefulclaritybot',
             'valkyribot',
             'gracefulcharitybot',
             'ddlbot',
             'NoSobStoryBot2',
             'bitofnewsbot',
             'conspirobot',
             'tipmoonbot1',
             'd3posterbot',
             'serendipitybot',
             'gabentipbot',
             'givesafuckbot',
             'SakuraiBot_test',
             'ttumblrbots',
             'haiku_robot',
             'tipmoonbot2',
             '[deleted]',
             'autotldr',
             'ConvertsToMetric',
             'ContentForager',
             'FonsoTheWhitesican',
             'imgurtranscriber',
             'Late_Night_Grumbler',
             'Lots42',
             'Lunas_Disciple',
             'MTGCardFetcher',
             'OriginalPostSearcher',
             'PriceZombie',
             'Removedpixel',
             'rollme',
             'rschaosid',
             'subredditreports.csv',
             'TotesMessenger',
             'TweetPoster',
             'User_Simulator',
             'TheNitromeFan',
             'sissyboi333',
             'atomicimploder',
             'AutoModerator',
             'ModerationLog',
             'qkme_transcriber',
             'original-finder',
             'red321red321',
             'SimilarImage',
             'iam4real',
             'red321red321',
             'rule34',
             'samacharbot2',
             'Mentioned_Videos',
             'mnemosyne-0000',
             'Lapis_Mirror',
             'XPostLinker',
             'untouchedURL']
# NOT ,             'system.indexes'
known_bots = set(bot_names)

if __name__ == '__main__':
    create_valid_user_set()
