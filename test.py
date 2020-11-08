
def convert_to_float(x):
    try:
        return float(x)
    except ValueError:
        return x

value = 'True'

print(convert_to_float(value))
print(type(convert_to_float(value)))

