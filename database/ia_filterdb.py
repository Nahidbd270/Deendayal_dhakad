import logging
from struct import pack
import re
import base64
import math
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from marshmallow.exceptions import ValidationError
from info import CAPTION_LANGUAGES, DATABASE_URI, DATABASE_URI2, DATABASE_NAME, COLLECTION_NAME, USE_CAPTION_FILTER, MAX_B_TN, DEENDAYAL_MOVIE_UPDATE_CHANNEL, OWNERID
from utils import get_settings, save_group_settings, temp, get_status
from database.users_chats_db import add_name
from .Imdbposter import get_movie_details, fetch_image
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
#---------------------------------------------------------
# Some basic variables needed
tempDict = {'indexDB': DATABASE_URI}

# Primary DB
client = AsyncIOMotorClient(DATABASE_URI)
db = client[DATABASE_NAME]
instance = Instance.from_db(db)

#secondary db
client2 = AsyncIOMotorClient(DATABASE_URI2)
db2 = client2[DATABASE_NAME]
instance2 = Instance.from_db(db2)


# Primary DB Model
@instance.register
class Media(Document):
    file_id = fields.StrField(attribute='_id')
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)

    class Meta:
        indexes = ('$file_name', )
        collection_name = COLLECTION_NAME

@instance2.register
class Media2(Document):
    file_id = fields.StrField(attribute='_id')
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)

    class Meta:
        indexes = ('$file_name', )
        collection_name = COLLECTION_NAME

async def choose_mediaDB():
    """This Function chooses which database to use based on the value of indexDB key in the dict tempDict."""
    global saveMedia
    # নিচের লাইনেই ভুলটি ছিল, যা এখন সংশোধন করা হয়েছে
    if tempDict['indexDB'] == DATABASE_URI:
        logger.info("Using first db (Media)")
        saveMedia = Media
    else:
        logger.info("Using second db (Media2)")
        saveMedia = Media2

async def save_file(bot, media):
  """Save file in database"""
  global saveMedia
  file_id, file_ref = unpack_new_file_id(media.file_id)
  
  file_name = re.sub(r"(_|\-|\.|\+|\(|\))", " ", str(media.file_name))
  
  try:
    if saveMedia == Media2: 
        if await Media.count_documents({'file_id': file_id}, limit=1):
            logger.warning(f'{file_name} is already saved in primary database!')
            return False, 0
    file = saveMedia(
        file_id=file_id,
        file_ref=file_ref,
        file_name=file_name,
        file_size=media.file_size,
        file_type=media.file_type,
        mime_type=media.mime_type,
        caption=media.caption.html if media.caption else None,
    )
  except ValidationError:
    logger.exception('Error occurred while saving file in database')
    return False, 2
  else:
    try:
      await file.commit()
    except DuplicateKeyError:
      logger.warning(f'{getattr(media, "file_name", "NO_FILE")} is already saved in database, but sending update post as requested.')
      if await get_status(bot.me.id):
          await send_msg(bot, file.file_name, file.caption, file.file_size)
      return False, 0
    else:
        logger.info(f'{getattr(media, "file_name", "NO_FILE")} is saved to database')
        if await get_status(bot.me.id):
            await send_msg(bot, file.file_name, file.caption, file.file_size)
        return True, 1

async def get_search_results(chat_id, query, file_type=None, max_results=10, offset=0, filter=False):
    """For given query return (results, next_offset)"""
    if chat_id is not None:
        settings = await get_settings(int(chat_id))
        try:
            if settings['max_btn']:
                max_results = 10
            else:
                max_results = int(MAX_B_TN)
        except KeyError:
            await save_group_settings(int(chat_id), 'max_btn', False)
            settings = await get_settings(int(chat_id))
            if settings['max_btn']:
                max_results = 10
            else:
                max_results = int(MAX_B_TN)
    query = query.strip()
    if not query:
        raw_pattern = '.'
    elif ' ' not in query:
        raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
    else:
        raw_pattern = query.replace(' ', r'.*[\s\.\+\-_()]')
    
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except:
        return []

    if USE_CAPTION_FILTER:
        filter = {'$or': [{'file_name': regex}, {'caption': regex}]}
    else:
        filter = {'file_name': regex}

    if file_type:
        filter['file_type'] = file_type

    total_results = ((await Media.count_documents(filter))+(await Media2.count_documents(filter)))

    if max_results%2 != 0: 
        logger.info(f"Since max_results is an odd number ({max_results}), bot will use {max_results+1} as max_results to make it even.")
        max_results += 1

    cursor = Media.find(filter)
    cursor2 = Media2.find(filter)
    cursor.sort('$natural', -1)
    cursor2.sort('$natural', -1)
    cursor2.skip(offset).limit(max_results)

    fileList2 = await cursor2.to_list(length=max_results)
    if len(fileList2)<max_results:
        next_offset = offset+len(fileList2)
        cursorSkipper = (next_offset-(await Media2.count_documents(filter)))
        cursor.skip(cursorSkipper if cursorSkipper>=0 else 0).limit(max_results-len(fileList2))
        fileList1 = await cursor.to_list(length=(max_results-len(fileList2)))
        files = fileList2+fileList1
        next_offset = next_offset + len(fileList1)
    else:
        files = fileList2
        next_offset = offset + max_results
    if next_offset >= total_results:
        next_offset = ''
    return files, next_offset, total_results

async def get_bad_files(query, file_type=None, filter=False):
    """For given query return (results, next_offset)"""
    query = query.strip()
    if not query:
        raw_pattern = '.'
    elif ' ' not in query:
        raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
    else:
        raw_pattern = query.replace(' ', r'.*[\s\.\+\-_()]')
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except:
        return []
    if USE_CAPTION_FILTER:
        filter = {'$or': [{'file_name': regex}, {'caption': regex}]}
    else:
        filter = {'file_name': regex}
    if file_type:
        filter['file_type'] = file_type
    cursor = Media.find(filter)
    cursor2 = Media2.find(filter)
    cursor.sort('$natural', -1)
    cursor2.sort('$natural', -1)
    files = ((await cursor2.to_list(length=(await Media2.count_documents(filter))))+(await cursor.to_list(length=(await Media.count_documents(filter)))))
    total_results = len(files)
    return files, total_results

async def get_file_details(query):
    filter = {'file_id': query}
    cursor = Media.find(filter)
    filedetails = await cursor.to_list(length=1)
    if not filedetails:
        cursor2 = Media2.find(filter)
        filedetails = await cursor2.to_list(length=1)
    return filedetails

def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(pack("<iiqq", int(decoded.file_type), decoded.dc_id, decoded.media_id, decoded.access_hash))
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref

def humanbytes(size):
    """Human-readable file size"""
    if not size:
        return ""
    power = 2**10
    n = 0
    Dic_powerN = {0: ' ', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return str(round(size, 2)) + " " + Dic_powerN[n] + 'B'

async def send_msg(bot, filename, caption, file_size): 
    try:
        clean_filename = re.sub(r'\@\S+|\@\S+|\b@\S+|\bwww\.\S+', '', filename).strip()
        clean_caption = re.sub(r'\@\S+|\@\S+|\b@\S+|\bwww\.\S+', '', caption or "").strip()
        
        year_match = re.search(r"\b(19|20)\d{2}\b", clean_caption)
        year = year_match.group(0) if year_match else None
        
        search_query = clean_filename
        if year:
            search_query = f"{clean_filename} {year}"
        
        imdb_data = await get_movie_details(search_query)  
        
        title = imdb_data.get('title', clean_filename)
        release_year = imdb_data.get('year', year or "N/A")
        genres = ", ".join(imdb_data.get('genres', [])) or "N/A"
        rating = imdb_data.get('rating', 'N/A')
        runtime = imdb_data.get('runtime', 'N/A')
        plot = imdb_data.get('plot', 'Not available.')
        imdb_url = imdb_data.get('url')
        poster_url = imdb_data.get('poster_url')

        qualities = ["ORG", "org", "hdcam", "HDCAM", "HQ", "hq", "HDRip", "hdrip", "camrip", "CAMRip", "hdtc", "predvd", "DVDscr", "dvdscr", "dvdrip", "HDTC", "dvdscreen", "HDTS", "hdts", "480p", "720p", "1080p", "2160p", "4K"]
        quality = await get_qualities(clean_caption.lower(), qualities) or "HDRip"

        language = ""
        possible_languages = CAPTION_LANGUAGES
        for lang in possible_languages:
            if lang.lower() in clean_caption.lower():
                language += f"{lang}, "
        language = language[:-2] if language else "Not Sure"

        text = (
            f"<b>✨ {title} ({release_year}) ✨</b>\n\n"
            f"<i>{plot}</i>\n\n"
            f"<b>🎭 Genre :</b> <code>{genres}</code>\n"
            f"<b>⭐ Rating :</b> <code>{rating}/10</code>\n"
            f"<b>⏳ Runtime :</b> <code>{runtime}</code>\n"
            f"<b>🔊 Language :</b> <code>{language}</code>\n"
            f"<b>💿 Quality :</b> <code>{quality}</code>\n"
            f"<b>🗂️ Size :</b> <code>{humanbytes(file_size)}</code>"
        )

        if await add_name(OWNERID, title):
            resized_poster = None
            if poster_url:
                resized_poster = await fetch_image(poster_url)

            btn = []
            file_link_name = title.replace(" ", '-')
            btn.append([InlineKeyboardButton('🌲 Get File 🌲', url=f"https://telegram.me/{temp.U_NAME}?start=getfile-{file_link_name}")])
            
            action_buttons = []
            if imdb_url:
                action_buttons.append(InlineKeyboardButton("🎬 IMDb", url=imdb_url))
            
            if action_buttons:
                btn.append(action_buttons)
            
            if resized_poster:
                await bot.send_photo(
                    chat_id=DEENDAYAL_MOVIE_UPDATE_CHANNEL, 
                    photo=resized_poster, 
                    caption=text, 
                    reply_markup=InlineKeyboardMarkup(btn)
                )
            else:              
                await bot.send_message(
                    chat_id=DEENDAYAL_MOVIE_UPDATE_CHANNEL, 
                    text=text, 
                    reply_markup=InlineKeyboardMarkup(btn),
                    disable_web_page_preview=True
                )

    except Exception as e:
        logger.error(f"[send_msg error] {e}", exc_info=True)

async def get_qualities(text, qualities: list):
    """Get all Quality from text"""
    quality_list = []
    for q in qualities:
        if q in text:
            quality_list.append(q)
    quality = ", ".join(quality_list)
    return quality
