import aiohttp
import asyncio
from datetime import datetime, timedelta
import hashlib
import json
import uuid

LOGIN_DATA = None
ZB_AUTH_BODY = {
  "apiVer": "v20",
  "authNumber": "",
  "countryCd": "KR",
  "locale": "ko_KR",
  "locationAgreeYn": "N",
  "mobileNum": "",
  "osVer": "13",
  "overwrite": True,
  "pushToken": "",
  "timeZone": int(datetime.now().astimezone().tzinfo.utcoffset(None).total_seconds() / 3600),
}
ZB_BASEURL = "https://iot.samsung-ihp.com:8088/openhome/"
ZB_HEADERS = {
  "Content-Type": "application/json",
  "Accept-Encoding": "gzip, deflate, br",
  "acceptLanguage": "ko_KR",
  "Host": "iot.samsung-ihp.com:8088",
  "User-Agent": "okhttp/4.2.1",
  "Authorization": "CUL ",
}

ZB_MEMBER_ID = None
ZB_DEVICE = {}
ZB_LAST_STAT = {}
ZB_TASK = []
ZB_SESSION = None
ZB_AUTH_RUNNING = False
ZB_AUTH_COND = asyncio.Condition()
ZB_CURRENT_STATE = None
ZB_SENSOR_FLAG = None

@service(supports_response = 'only')
def zb_init(id, password, sensors = None, prefix = "homeassistant", imei = None):
  """yaml
name: 도어락 등록
description: 직방 도어락을 홈어시스턴트에 등록합니다
fields:
  id:
    description: 직방아이디
    example: zigbang
    required: true
  password:
    description: 직방암호
    example: zigbang
    required: true
  sensors:
    description: 도어센서
    example: binary_sensor.entry_door
    required: false
  prefix:
    description: MQTT Discovery Prefix
    example: homeassistant
    required: false
  imei:
    description: 직방 로그온을 위한 IMEI 정보
    example: 000000000000000
    required: false
"""
  global LOGIN_DATA, ZB_TASK, ZB_CURRENT_STATE, ZB_SENSOR_FLAG
  task.unique("ZB_DOORLOCK_INIT", kill_me = True)
  LOGIN_DATA = {
    "loginId": id,
    "pwd": zb_hash(password),
    "imei": imei if imei is not None else zb_get_imei(),
  }

  try:
    # INIT
    zb_shutdown()
    zb_get_appver()
    zb_auth()
    ZB_CURRENT_STATE = zb_get_status(True)
    for index, _ in enumerate(ZB_CURRENT_STATE):
      ZB_LAST_STAT[index] = {}

    # ADD DEVICE
    for index in ZB_DEVICE:
      payload = {
        "optimistic": False,
        "qos": 0,
        "device": {
          "identifiers": [ZB_DEVICE[index]["id"]],
          "name": ZB_DEVICE[index]["name"],
          "model": ZB_DEVICE[index]["model"],
          "manufacturer": "Zigbang Doorlock (3735943886)",
          "sw_version": "0.2",
        },
      }
      payload["name"] = ZB_DEVICE[index]["name"]
      payload["unique_id"] = ZB_DEVICE[index]["id"] + "_lock"
      payload["object_id"] = ZB_DEVICE[index]["model"]
      payload["state_topic"] = "zigbang/{}/locked".format(ZB_DEVICE[index]["id"])
      payload["command_topic"] = "zigbang/command/{}".format(ZB_DEVICE[index]["id"])
      payload["state_locked"] = True
      payload["state_unlocked"] = False
      mqtt.publish(topic = "{}/lock/{}/config".format(prefix, payload["unique_id"]), payload = json.dumps(payload))

      payload["name"] = ZB_DEVICE[index]["name"] + "배터리"
      payload["unique_id"] = ZB_DEVICE[index]["id"] + "_battery"
      payload["object_id"] = ZB_DEVICE[index]["model"] + "_battery"
      payload["state_topic"] = "zigbang/{}/battery".format(ZB_DEVICE[index]["id"])
      del payload["command_topic"]
      del payload["state_locked"]
      del payload["state_unlocked"]
      payload["device_class"] = "battery"
      payload["unit_of_measurement"] = "%"
      mqtt.publish(topic = "{}/sensor/{}/config".format(prefix, payload["unique_id"]), payload = json.dumps(payload))

      payload["name"] = ZB_DEVICE[index]["name"] + "메시지"
      payload["unique_id"] = ZB_DEVICE[index]["id"] + "_message"
      payload["object_id"] = ZB_DEVICE[index]["model"] + "_message"
      payload["icon"] = "mdi:message-alert"
      payload["state_topic"] = "zigbang/{}/msgText".format(ZB_DEVICE[index]["id"])
      del payload["device_class"]
      del payload["unit_of_measurement"]
      mqtt.publish(topic = "{}/sensor/{}/config".format(prefix, payload["unique_id"]), payload = json.dumps(payload))

    task.sleep(2)
    for index, _ in enumerate(ZB_CURRENT_STATE):
      zb_data_refine(ZB_DEVICE[index]["id"], ZB_CURRENT_STATE[index], ZB_LAST_STAT[index])
    if sensors:
      ZB_SENSOR_FLAG = True
      if not isinstance(sensors, list):
        sensors = [sensors]
      for sensor in sensors:
        ZB_TASK.append(add_loop_state(sensor, 10, "ZB_DOORLOCK_LOOP", 2, False))
    else:
      ZB_SENSOR_FLAG = False
    return { "doorlock": ZB_DEVICE }
  except aiohttp.ClientResponseError as e:
    if e.status == 401:
      error_message = "인증 실패: 아이디, 비밀번호, 또는 imei를 확인해주세요."
    else:
      error_message = f"서버 응답 오류 (HTTP {e.status}): {e.message}"
    return { "error": error_message }
  except (aiohttp.ClientConnectorError, asyncio.TimeoutError):
    return { "error": "네트워크 오류: 직방 서버에 연결할 수 없습니다. 인터넷 연결을 확인하거나 잠시 후 다시 시도해주세요." }
  except (KeyError, TypeError) as e:
    return { "error": f"데이터 처리 오류: 직방 API 응답 형식이 변경되었을 수 있습니다. 오류: {e}" }
  except Exception as e:
    return { "error": f"알 수 없는 오류 발생: {type(e).__name__}: {str(e)}" }


@time_trigger("cron(*/1 * * * *)")
def zb_lock_check(**kwargs):
  global ZB_CURRENT_STATE, ZB_SENSOR_FLAG
  task.unique("ZB_PERIOD_LOCK_CHECK")
  skip_poll = ZB_SENSOR_FLAG
  if ZB_CURRENT_STATE:
    if skip_poll:
      for index, _ in enumerate(ZB_CURRENT_STATE):
        if not ZB_CURRENT_STATE[index].get("locked", True):
          skip_poll = False
          break
    if not skip_poll:
      zb_loop_internal(1, "ZB_DOORLOCK_LOOP", 0, True)


@mqtt_trigger("zigbang/command/#")
def zb_lock_command(**kwargs):
  targetdev = kwargs["topic"].split("/")[2]
  command = kwargs["payload"]
  if command == "UNLOCK":
    task.create(zb_loop_internal, 15, "ZB_DOORLOCK_LOOP", 2, False)
    zb_unlock(targetdev)


def zb_data_refine(id, data, olddata):
  global ZB_AUTH_BODY
  for key in data:
    if key not in olddata or data[key] != olddata[key]:
      payload = None
      olddata[key] = data[key]
      if key == "locked":
        payload = data[key]
      elif key == "battery":
        payload = zb_battery(data[key])
      elif key == "rgstDt":
        event_date = datetime.strptime(data["rgstDt"], "%Y-%m-%d %H:%M:%S") + timedelta(hours = ZB_AUTH_BODY["timeZone"])
        payload = "{} {}".format(event_date.strftime("%Y-%m-%d %H:%M:%S"), data["msgText"])
        key = "msgText"
      if payload is not None:
        mqtt.publish(topic = "zigbang/{}/{}".format(id, key), payload = payload)

def zb_loop_internal(timeout, unique, interval, kill_me):
  global ZB_LAST_STAT, ZB_CURRENT_STATE
  task.unique(unique, kill_me)
  for _ in range(0, timeout):
    try:
      ZB_CURRENT_STATE = zb_get_status()
      for index, _ in enumerate(ZB_CURRENT_STATE):
        zb_data_refine(ZB_DEVICE[index]["id"], ZB_CURRENT_STATE[index], ZB_LAST_STAT[index])
    except Exception as e:
      log.warning("{}: {}".format(type(e).__name__, str(e)))
    task.sleep(interval)

def add_loop_state(trigger, timeout, unique, interval, kill_me):
  @state_trigger(trigger)
  def zb_loop(**kwargs):
    zb_loop_internal(timeout, unique, interval, kill_me)
  return zb_loop


@pyscript_compile
async def zb_get_status(getdevice = False):
  global ZB_MEMBER_ID, ZB_DEVICE
  if ZB_MEMBER_ID == None:
    await zb_auth()
  url = "v20/doorlockctrl/membersdoorlocklist?createDate={}&favoriteYn=A&hashData=&memberId={}"
  url = url.format(zb_createdate(), ZB_MEMBER_ID)
  res = await zb_request(url, "GET")
  result = []
  for index, stat in enumerate(res["doorlockVOList"]):
    result.append({
      "locked": stat["doorlockStatusVO"]["locked"],
      "battery": stat["doorlockStatusVO"]["battery"],
      "rgstDt": stat["recentHistoryVOList"]["rgstDt"],
      "msgText": stat["recentHistoryVOList"]["msgText"],
    })
    if getdevice:
      ZB_DEVICE[index] = {
        "id": res["doorlockVOList"][index]["deviceId"],
        "name": res["doorlockVOList"][index]["deviceNm"],
        "model": res["doorlockVOList"][index]["productId"],
      }
  return result

@pyscript_compile
async def zb_unlock(deviceid):
  global ZB_MEMBER_ID
  if ZB_MEMBER_ID == None:
    await zb_auth()
  data = {
    "createDate": zb_createdate(),
    "deviceId": deviceid,
    "open": True,
    "isSecurityMode": False,
    "memberId": ZB_MEMBER_ID,
    "securityModeRptEndDt": "",
    "securityModeRptStartDt": "",
  }
  zb_add_hash(data)
  return await zb_request("v20/doorlockctrl/open", "PUT", data = data)

@pyscript_compile
async def zb_get_appver():
  global ZB_HEADERS, ZB_AUTH_BODY
  if "Authorization" in ZB_HEADERS:
    ZB_HEADERS["Authorization"] = "CUL "
  if "AuthCode" in ZB_HEADERS:
    del ZB_HEADERS["AuthCode"]
  response = await zb_request("v20/appsetting/getappver?createDate={}&hashData=&osTypeCd=iOS%20".format(zb_createdate()), "GET")
  ZB_AUTH_BODY["appVer"] = response["AppVersionList"][0]["osAppVer"]
  ZB_AUTH_BODY["osTypeCd"] = response["AppVersionList"][0]["osTypeCd"]
  return response

@pyscript_compile
async def zb_auth():
  global ZB_AUTH_BODY, ZB_HEADERS, ZB_MEMBER_ID, ZB_AUTH_RUNNING, ZB_AUTH_COND
  async with ZB_AUTH_COND:
    if ZB_AUTH_RUNNING:
      try:
        await asyncio.wait_for(ZB_AUTH_COND.wait(), timeout = 30)
      finally:
        return
    ZB_AUTH_RUNNING = True
  try:
    if "Authorization" in ZB_HEADERS:
      ZB_HEADERS["Authorization"] = "CUL "
    if "AuthCode" in ZB_HEADERS:
      del ZB_HEADERS["AuthCode"]
    if "appVer" not in ZB_AUTH_BODY:
      await zb_get_appver()
    ZB_AUTH_BODY["createDate"] = zb_createdate()
    ZB_AUTH_BODY.update(LOGIN_DATA)
    zb_add_hash(ZB_AUTH_BODY)
    response = await zb_request("v10/user/login", "PUT", data = ZB_AUTH_BODY)
    ZB_HEADERS["Authorization"] = "CUL " + response["authToken"]
    ZB_HEADERS["AuthCode"] = response["authCode"]
    ZB_MEMBER_ID = response["memberId"]
  finally:
    async with ZB_AUTH_COND:
      ZB_AUTH_RUNNING = False
      ZB_AUTH_COND.notify_all()
  return

@pyscript_compile
async def zb_request(url, method, data = None):
  global ZB_HEADERS, ZB_BASEURL
  kwargs = { "headers": ZB_HEADERS }
  if method in ("PUT", "POST") and data is not None:
    kwargs["json"] = data
  session = zb_session(ZB_BASEURL)
  async with session.request(method, url, **kwargs) as response:
    if response.status != 401 or url == "v10/user/login":
      response.raise_for_status()
      return await response.json()
  await zb_auth()
  async with session.request(method, url, **kwargs) as response:
    response.raise_for_status()
    return await response.json()

@pyscript_compile
def zb_add_hash(data):
  data["hashData"] = zb_hash("".join([str(i) for i in data.values()]))

@pyscript_compile
def zb_get_imei():
  return str(uuid.getnode())

@pyscript_compile
def zb_hash(text):
  return hashlib.sha512(text.encode()).hexdigest()

@pyscript_compile
def zb_createdate():
  return datetime.now().strftime("%Y%m%d%H%M%S")

@pyscript_compile
def zb_battery(battery):
  return next((value for threshold, value in [(60, 100), (55, 80), (52, 60), (50, 40)] if battery >= threshold), 20)

@pyscript_compile
def zb_session(base_url):
  global ZB_SESSION
  if ZB_SESSION is None or ZB_SESSION.closed:
    ZB_SESSION = aiohttp.ClientSession(base_url = base_url)
  return ZB_SESSION

@time_trigger("shutdown")
def zb_shutdown():
  global ZB_SESSION, ZB_TASK
  task.unique("ZB_DOORLOCK_LOOP")
  task.unique("ZB_PERIOD_LOCK_CHECK")
  ZB_TASK = []
  if ZB_SESSION and not ZB_SESSION.closed:
    try:
      ZB_SESSION.close()
    finally:
      ZB_SESSION = None
