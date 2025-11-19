import joblib

path = r"F:\autostockG\HOJ_ENGINE_REAL_V25.pkl"

model = joblib.load(path)

print("=== 엔진 타입 ===")
print(type(model))

print("\n=== 엔진 내용 ===")
print(model)

print("\n=== 엔진 DIR ===")
print(dir(model))
