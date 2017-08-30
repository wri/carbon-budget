pixels = [0, 1, 2, 3]

for p in pixels:
    if p == 1 or p == 0:
        out1 = 2
    else:
        out1 = 0
        
    if p == 2 or p == 0:
        out2 = 2
    else:
        out2 = 0
        
    if p == 3 or p == 0:
        out3 = 2
    else:
        out3 = 0
        
    final_out = out1 + out2 + out3
    
    print final_out