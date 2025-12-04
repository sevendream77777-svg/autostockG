<?php
header("Content-Type: application/json; charset=utf-8");

// code 값이 없으면 종료
if (!isset($_GET['code'])) {
    echo json_encode(null);
    exit;
}

$code = preg_replace('/[^0-9]/', '', $_GET['code']);
$url = "https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:" . $code;

// 네이버 API 호출
$ch = curl_init();
curl_setopt($ch, CURLOPT_URL, $url);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
$result = curl_exec($ch);
curl_close($ch);

if (!$result) {
    echo json_encode(null);
    exit;
}

$data = json_decode($result, true);

// 구조 파싱
$item = $data["result"]["areas"][0]["datas"][0] ?? null;

if (!$item) {
    echo json_encode(null);
    exit;
}

// p6에서 쓰는 형태로 변환
echo json_encode([
    "price"  => $item["closePrice"] ?? null,
    "change" => $item["compareToPreviousClosePrice"] ?? null,
    "volume" => $item["accumulatedTradingVolume"] ?? null
], JSON_UNESCAPED_UNICODE);
