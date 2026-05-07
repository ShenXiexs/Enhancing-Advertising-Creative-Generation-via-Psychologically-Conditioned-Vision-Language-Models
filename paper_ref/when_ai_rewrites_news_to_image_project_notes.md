# 从 "When AI Rewrites the News" 到我们的 LLM 商品图研究

> 参考论文：Khatiwada, Pappu, Bagozzi, & Mauriello. 2026. *When AI Rewrites the News: How Sentiment, Framing, and LLM Disclosure Shape Perceptions*. CHI 2026. DOI: 10.1145/3772318.3791527

## 1. 一句话理解这篇论文

这篇论文研究的是：当 LLM 改写新闻文本时，改变文本的情绪强度、叙事框架，以及是否告知读者 "这篇文章被 LLM 修改过"，会怎样影响读者对新闻的偏见、可信度、情绪反应和伦理担忧。

对我们最有启发的地方不是新闻本身，而是它把 "AI 改写内容" 拆成了三个可实验操控的层次：

1. **内容表层风格**：sentiment，文本情绪强度。
2. **意义组织方式**：framing，信息如何被选择、强调和组织。
3. **AI 介入可见性**：disclosure，用户是否知道内容被 AI 改过。

我们的项目可以把这个框架迁移到商品图：

1. **视觉情绪强度**：颜色、光线、材质、对比度、戏剧性、背景氛围。
2. **视觉 framing**：商品被放进什么生活场景、价值语境、身份叙事或消费想象中。
3. **AI / personalization disclosure**：用户是否知道图片由 AI 生成、是否知道图片按人格/价值观定制。

核心迁移句：

> 他们研究 "LLM 如何通过改写新闻语言改变读者判断"；我们可以研究 "LLM 如何通过改写商品图的视觉语境改变消费者判断"。

## 2. 论文的研究设计

论文使用一个 2x2 between-subjects 实验，加一个原始新闻 baseline。

| 维度 | 水平 | 含义 |
|---|---|---|
| Sentiment | Neutral vs. Extreme | 中性事实语气 vs. 情绪化、评价性强的语气 |
| Framing | Balanced vs. One-sided | 多方观点平衡呈现 vs. 强调单一立场 |
| Baseline | Original article | 未经 LLM 修改的原始新闻 |
| Disclosure | Pre vs. post | 先不告诉 AI 修改，再告知并重新评价 |

样本为 180 名美国 MTurk 参与者进入 2x2 条件，每格约 45 人；另外 45 人阅读原始新闻作为 baseline，总 N=225。

测量变量包括：

| 类型 | 变量 |
|---|---|
| 可信/判断 | perceived bias, trustworthiness, argument imbalance |
| 情绪反应 | anger, disgust, resentment, anxiety, surprise, happiness |
| AI 修改感知 | 是否扭曲、遗漏、引入 bias、夸大、改变主旨 |
| 伦理/意图 | 是否操纵、是否带 agenda、是否 ethically questionable |
| 开放题 | 是否接受 AI 新闻、是否需要 disclosure、人类监督的角色 |

## 3. 论文的方法论亮点

### 3.1 LLM 不是直接生成最终刺激，而是生成候选刺激

他们先选真实新闻，再用 GPT/Grok 生成 4 个版本，之后用 Claude/Gemini 和人类专家验证。这点很重要：LLM 在研究里不是 "黑箱一次性生成器"，而是 controlled manipulation pipeline 的一部分。

对我们来说，商品图也不应该只是 "跑 ComfyUI 出图然后直接实验"。更稳的说法应该是：

> We use LLMs and image-generation models to produce controlled visual transformations, then validate whether the resulting images actually instantiate the intended visual-affective and persona-framing conditions.

### 3.2 他们区分了 construct 和 surface realization

论文没有把 sentiment 简化成某几个词，而是定义为 "affective valence and intensity of evaluative language"。具体的形容词、引用、修辞是模型自然产生的 surface pattern。

对应到我们这里：

| 抽象 construct | 视觉 surface realization |
|---|---|
| Visual sentiment / affective intensity | 色彩温度、明暗对比、光影戏剧性、材质、背景密度、道具情绪 |
| Visual framing | 场景类型、商品使用情境、生活方式、身份线索、价值观符号 |
| Persona alignment | MBTI / Big Five / Schwartz 对应的审美、场景和消费动机 |

我们可以避免说 "红色就是 extroversion" 这种硬编码，而是说人格提示诱导模型生成一组视觉策略，再通过人工和自动指标验证。

### 3.3 他们做了多层 validation

论文验证 LLM 改写刺激的方式：

| 验证层 | 方法 |
|---|---|
| LLM validation | 用不同模型检查 tone/framing 是否符合条件 |
| Human coding | 政治学和内容分析专家打分 |
| Automated metrics | VADER 测 sentiment；stance entropy 测 framing diversity |
| Pilot screening | 选出最稳定、最可信、最少事实漂移的一篇新闻 |

我们可以对应设计：

| 验证层 | 商品图版本 |
|---|---|
| LLM/CV validation | 用 VLM 判断图片是否符合 persona、场景、情绪强度 |
| Human coding | 编码 product fidelity、background-persona fit、visual appeal、manipulativeness |
| Automated metrics | CLIP/image similarity、商品区域大小、颜色分布、亮度/饱和度、背景复杂度、OCR/no text |
| Pilot screening | 选出商品未变形、背景足够可控、persona 差异明显的类目/商品 |

## 4. 论文的主要发现

### 4.1 Sentiment 是最强的 lever

极端情绪语气更容易提高负面情绪和 perceived bias，并降低 trustworthiness。Framing 的影响更细、更条件化。

转译到我们项目：

> 商品图里的 "视觉情绪强度" 可能比 "人格标签本身" 更直接影响消费者反应。

比如高对比、强氛围、奢华/冒险/焦虑/安全感等视觉线索，可能比 MBTI 四个字母本身更能驱动 purchase intention、trust、comfort、quality perception。

### 4.2 Balanced + Extreme 可能 backfire

论文发现一个很有意思的点：balanced framing 如果搭配 extreme sentiment，反而可能让人觉得更 biased、更 surprising。这和 hostile media effect 相关：看似平衡的内容，如果两边都带强情绪，会让读者更容易感到不适或怀疑。

对商品图的启发：

> "适配多个用户心理动机" 不一定更安全。如果图像同时塞入多个强烈价值符号，可能会显得更假、更操纵、更不可信。

例子：

| 图像策略 | 潜在问题 |
|---|---|
| 同时强调奢华、环保、家庭、安全、冒险 | 视觉语义冲突，显得像过度营销 |
| 人格 cue 太强 | 用户感到被分类或被操纵 |
| 背景太 "懂我" | personalization creepiness |
| 情绪化背景但商品本身普通 | 商品可信度下降 |

### 4.3 Disclosure 的作用有限且条件化

论文里，告知 AI 修改并没有全面改变 bias/fairness 判断，但会让 trustworthiness 轻微下降，尤其是文章已经 extreme 或 one-sided 时。

对商品图：

> "AI-generated" 或 "personalized for you" 标签可能不会简单地降低所有评价；它更可能在图片已经显得夸张、操纵或不真实时放大不信任。

这提示我们可以设计 disclosure 条件：

| Disclosure 条件 | 示例 |
|---|---|
| No disclosure | 直接展示商品图 |
| AI-generated disclosure | "This product image was generated/edited by AI." |
| Personalization disclosure | "This image was customized based on your style/personality preferences." |
| Process disclosure | "AI changed only the background; product appearance was preserved." |

最有研究价值的可能不是简单 AI label，而是：

> AI label vs. process label vs. personalization label 的差异。

### 4.4 Subtle modification 可能比 overt bias 更让人怀疑

论文里一个反直觉发现是：Neutral + Balanced 有时反而引发更高的操纵/伦理担忧，因为它看起来更 "不透明"。明显偏激的内容容易被识别和 discount；微妙的平衡改写反而可能让人担心隐藏 agenda。

对我们项目很关键：

> 轻微、自然、审美上合理的 AI 背景修改，未必一定比明显个性化修改更安全。用户可能会觉得 "看起来不像广告，但其实在影响我"。

这可以直接变成我们的研究问题：

> Are subtle AI-generated product backgrounds perceived as less manipulative, or do they increase suspicion because the persuasive intent is harder to detect?

## 5. 我们可以如何重新定义自己的研究

### 5.1 从 "人格生成图片" 升级为 "AI-mediated visual framing"

当前项目名和 pipeline 容易被理解为：用 MBTI/Big Five/Schwartz 生成商品背景图。

但从论文框架看，更学术、更可发表的定义是：

> This project studies how LLM-mediated visual framing changes consumer perception of e-commerce products.

人格不是唯一贡献，而是 "visual framing strategy" 的一种来源：

| 来源 | 作用 |
|---|---|
| 商品类目 | 控制基本商业语境 |
| MBTI | 生成 identity/personality-oriented visual framing |
| Big Five | 生成 trait-based visual affect and lifestyle cues |
| Schwartz | 生成 value-based consumption framing |
| No persona baseline | 一般商品图背景生成 |

### 5.2 可用论文标题方向

1. **When AI Restyles Products: How Personality-Based Visual Framing and Disclosure Shape Consumer Perceptions**
2. **When LLMs Rewrite Product Images: Personality-Congruent Backgrounds, Visual Affect, and Consumer Trust**
3. **AI-Mediated Visual Framing in E-Commerce: How Persona-Driven Product Images Shape Appeal, Trust, and Manipulation Concerns**
4. **From White Backgrounds to Personalized Scenes: The Perceptual Effects of LLM-Generated Product Image Contexts**

中文核心题目可以是：

> 当 AI 改写商品图：人格化视觉 framing 如何影响消费者的信任、偏好与操纵感知

## 6. 我们的变量可以怎么设计

### 6.1 Independent variables

| 论文变量 | 商品图对应变量 | 可能水平 |
|---|---|---|
| Sentiment | Visual affective intensity | neutral / emotionally intense |
| Framing | Visual framing | product-centric / persona- or value-centric |
| Disclosure | AI / personalization disclosure | none / AI-edited / personalized / process disclosure |
| Baseline | Original image | white-background product image |

### 6.2 Dependent variables

| 构念 | 商品图问法 |
|---|---|
| Product appeal | How appealing is this product? |
| Purchase intention | How likely would you be to click/buy? |
| Trustworthiness | How trustworthy does this product image feel? |
| Product quality | How high-quality does the product seem? |
| Perceived fit | How well does the background fit the product? |
| Personal relevance | How relevant does this image feel to you? |
| Manipulation concern | Does the image feel designed to influence you? |
| Authenticity | Does the image feel authentic or artificial? |
| AI distortion | Does the image seem to misrepresent the product? |
| Emotional response | calm, excited, anxious, inspired, skeptical |
| Disclosure attitude | Are you comfortable with AI-personalized product images? |

### 6.3 Visual validation rubric

可以模仿论文 Table 1，建立我们的人工评分表：

| Metric | Description |
|---|---|
| Product fidelity | 商品外观、形状、颜色、比例是否保持 |
| Background quality | 背景是否自然、清晰、无明显生成错误 |
| Persona alignment | 背景是否符合目标人格/价值观 |
| Category fit | 背景是否适合商品类别 |
| Visual affect intensity | 情绪/氛围强度是否符合条件 |
| Framing clarity | 图片是否明确传达某种生活方式/价值语境 |
| Manipulativeness | 是否显得过度营销或操纵 |
| Realism | 是否像真实可用的电商图 |
| Overall effectiveness | 是否达成目标视觉沟通意图 |

## 7. 可执行实验方案

### 7.1 最小可发表版本

目标：先证明 "LLM-generated visual context changes product perception"。

设计：

| 条件 | 图片 |
|---|---|
| Baseline | 白底商品图 |
| Generic AI background | 无 persona 的商品背景图 |
| Persona-congruent background | 根据参与者人格/价值观生成的背景图 |
| Persona-incongruent background | 与参与者人格/价值观不匹配的背景图 |

关键 DV：

1. appeal
2. purchase intention
3. trustworthiness
4. personalization fit
5. manipulation concern
6. product misrepresentation concern

优点：和你现有 pipeline 最接近，马上能做。

### 7.2 论文对齐版 2x2

更贴近参考论文：

| 维度 | 水平 |
|---|---|
| Visual affect | neutral vs. intense |
| Visual framing | product-centric vs. persona/value-centric |
| Disclosure | no disclosure vs. AI/personality disclosure |
| Baseline | white-background image |

可能假设：

1. Intense visual affect 会提高 excitement/appeal，但也可能提高 manipulation concern。
2. Persona/value-centric framing 会提高 personal relevance，但在 disclosure 后可能增加 creepiness。
3. AI disclosure 对 trust 的影响取决于图片是否已经显得 intense 或 over-personalized。
4. Neutral-looking but personalized images 可能比 overtly stylized images 更容易引发隐性操纵担忧。

### 7.3 人格系统比较版

如果重点放在 MBTI / Big Five / Schwartz：

| 条件 | 含义 |
|---|---|
| No persona | 类目驱动背景 |
| MBTI | 类型人格驱动背景 |
| Big Five | trait 强弱驱动背景 |
| Schwartz | 价值观驱动背景 |

这个版本的研究问题不是 "哪个更好"，而是：

> Different psychological models generate different visual framing strategies; these strategies may differ in appeal, trust, and perceived manipulation.

这比比较 MBTI/Big Five/Schwartz 谁更准确更稳，因为我们真正测的是它们作为 prompt framing systems 的差异。

## 8. 这篇论文给我们的理论借口

可以引入几个理论桥：

| 论文理论 | 我们的视觉版本 |
|---|---|
| Framing theory | 商品被置于不同视觉语境中，改变消费者如何理解商品价值 |
| Sentiment/emotional tone | 视觉情绪强度改变情绪反应和购买动机 |
| Hostile Media Effect | 当视觉看似平衡/自然但情绪或价值 cue 很强时，用户可能感到被操纵 |
| Algorithmic disclosure | AI 介入标签不只影响信任，也改变用户对图像意图的解释 |
| Transparency beyond labels | 不只标 "AI 生成"，还要说明 AI 改了什么、商品本身是否被改 |

我们可以把核心理论句写成：

> Product images do not merely display products; they frame products. When LLMs generate the visual context around a product, they can alter the affective, identity-related, and value-laden meanings attached to that product.

## 9. 需要特别小心的地方

### 9.1 不要把 persona 当成心理学真值

我们不需要证明 MBTI 真的能预测消费者。更稳的说法是：

> MBTI, Big Five, and Schwartz values are used as prompt-based audience models that structure visual framing.

这样可以避免被审稿人攻击 "MBTI 不科学"。我们的贡献是研究 "persona prompt framing" 对视觉生成和消费者感知的影响。

### 9.2 商品 fidelity 是底线

新闻论文关注事实是否被扭曲。我们对应的是商品是否被 misrepresented。

必须测：

1. 商品形状是否变了。
2. 商品颜色/材质是否变了。
3. 背景是否暗示了不存在的功能或使用场景。
4. 用户是否觉得图片夸大了商品质量。

### 9.3 不要只测美观

如果只测 aesthetic appeal，会变成普通广告设计实验。需要加入：

1. trust
2. authenticity
3. manipulation concern
4. product misrepresentation
5. AI disclosure comfort

这些才让它成为 HCI / AI-mediated content / consumer perception 研究。

### 9.4 单商品 vs 多商品的 trade-off

论文为了 internal validity 只选一篇新闻，但承认外部效度有限。

我们的选择：

| 方案 | 优点 | 风险 |
|---|---|---|
| 少数商品、强验证 | 条件干净，易解释 | 类目泛化弱 |
| 多类目、多商品 | 更像真实电商 | 噪声大，图像质量不稳定 |

比较务实的路线：

1. Pilot 用 10 个商品筛选稳定类目。
2. 正式实验用 3-5 个类目，每类 4-6 个商品。
3. 商品作为 random effect 或至少在分析中控制。

## 10. 下一步可以怎么落地

### 10.1 先做 stimulus audit

从现有输出中抽样：

1. no persona
2. MBTI
3. Big Five
4. Schwartz

每类挑 20-40 张图，建立人工评分表：

1. 商品是否保持。
2. 背景是否自然。
3. persona 是否可感知。
4. 是否过度营销。
5. 是否有操纵感。

目标不是马上上用户实验，而是先证明 stimuli 有 construct validity。

### 10.2 做一个小型 perception pilot

每个参与者看 8-12 张图，避免疲劳。

每张图问：

1. appeal
2. purchase intention
3. trustworthiness
4. authenticity
5. background fit
6. manipulation concern
7. AI-generated guess

最后问 disclosure：

1. 如果知道背景由 AI 生成，你的信任是否改变？
2. 如果知道背景按人格/价值观定制，你是否舒服？
3. 你是否希望看到原始白底图？

### 10.3 写作上的主线

可能的 Introduction 逻辑：

1. 电商商品图正在从白底展示走向 AI 生成的情境化展示。
2. LLM 不只是生成文字 prompt，也在决定商品被放入什么视觉语境。
3. 这些视觉语境可能改变消费者对商品质量、可信度和个人相关性的判断。
4. 现有研究关注 AI 新闻、AI 文本和 disclosure，但较少研究 LLM-mediated product imagery。
5. 本研究考察 persona-driven visual framing 如何影响消费者感知，以及 AI/personalization disclosure 是否改变这些影响。

## 11. 可以直接形成的研究问题

RQ1. How do LLM-generated product image backgrounds alter consumer perceptions compared with original white-background product images?

RQ2. Do persona-driven visual framings, based on MBTI, Big Five, or Schwartz values, increase perceived personal relevance and product appeal?

RQ3. Do stronger visual-affective cues increase appeal at the cost of trust, authenticity, or perceived manipulation?

RQ4. How does disclosure of AI generation or persona-based personalization affect trust in AI-modified product images?

RQ5. Are subtle persona-congruent visual modifications perceived as less manipulative, or do they produce greater suspicion because the persuasive intent is less visible?

## 12. 最值得带走的一句话

这篇新闻论文给我们的最大启发是：

> AI 修改内容的风险和价值，不只在于 "有没有生成错误"，而在于它如何重新组织情绪、语境和解释框架。

放到我们的商品图项目里，就是：

> LLM 生成的商品背景不是装饰，而是视觉 framing。它可能提高吸引力和个人相关性，也可能改变信任、真实性和操纵感。

