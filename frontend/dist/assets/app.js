(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const r of document.querySelectorAll('link[rel="modulepreload"]'))s(r);new MutationObserver(r=>{for(const a of r)if(a.type==="childList")for(const o of a.addedNodes)o.tagName==="LINK"&&o.rel==="modulepreload"&&s(o)}).observe(document,{childList:!0,subtree:!0});function t(r){const a={};return r.integrity&&(a.integrity=r.integrity),r.referrerPolicy&&(a.referrerPolicy=r.referrerPolicy),r.crossOrigin==="use-credentials"?a.credentials="include":r.crossOrigin==="anonymous"?a.credentials="omit":a.credentials="same-origin",a}function s(r){if(r.ep)return;r.ep=!0;const a=t(r);fetch(r.href,a)}})();const M=globalThis,z=M.ShadowRoot&&(M.ShadyCSS===void 0||M.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,I=Symbol(),K=new WeakMap;let re=class{constructor(e,t,s){if(this._$cssResult$=!0,s!==I)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=t}get styleSheet(){let e=this.o;const t=this.t;if(z&&e===void 0){const s=t!==void 0&&t.length===1;s&&(e=K.get(t)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),s&&K.set(t,e))}return e}toString(){return this.cssText}};const de=i=>new re(typeof i=="string"?i:i+"",void 0,I),ce=(i,...e)=>{const t=i.length===1?i[0]:e.reduce((s,r,a)=>s+(o=>{if(o._$cssResult$===!0)return o.cssText;if(typeof o=="number")return o;throw Error("Value passed to 'css' function must be a 'css' function result: "+o+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(r)+i[a+1],i[0]);return new re(t,i,I)},he=(i,e)=>{if(z)i.adoptedStyleSheets=e.map(t=>t instanceof CSSStyleSheet?t:t.styleSheet);else for(const t of e){const s=document.createElement("style"),r=M.litNonce;r!==void 0&&s.setAttribute("nonce",r),s.textContent=t.cssText,i.appendChild(s)}},V=z?i=>i:i=>i instanceof CSSStyleSheet?(e=>{let t="";for(const s of e.cssRules)t+=s.cssText;return de(t)})(i):i;const{is:pe,defineProperty:ue,getOwnPropertyDescriptor:ge,getOwnPropertyNames:me,getOwnPropertySymbols:be,getPrototypeOf:fe}=Object,H=globalThis,Z=H.trustedTypes,ve=Z?Z.emptyScript:"",$e=H.reactiveElementPolyfillSupport,C=(i,e)=>i,j={toAttribute(i,e){switch(e){case Boolean:i=i?ve:null;break;case Object:case Array:i=i==null?i:JSON.stringify(i)}return i},fromAttribute(i,e){let t=i;switch(e){case Boolean:t=i!==null;break;case Number:t=i===null?null:Number(i);break;case Object:case Array:try{t=JSON.parse(i)}catch{t=null}}return t}},B=(i,e)=>!pe(i,e),G={attribute:!0,type:String,converter:j,reflect:!1,useDefault:!1,hasChanged:B};Symbol.metadata??=Symbol("metadata"),H.litPropertyMetadata??=new WeakMap;let A=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??=[]).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,t=G){if(t.state&&(t.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((t=Object.create(t)).wrapped=!0),this.elementProperties.set(e,t),!t.noAccessor){const s=Symbol(),r=this.getPropertyDescriptor(e,s,t);r!==void 0&&ue(this.prototype,e,r)}}static getPropertyDescriptor(e,t,s){const{get:r,set:a}=ge(this.prototype,e)??{get(){return this[t]},set(o){this[t]=o}};return{get:r,set(o){const l=r?.call(this);a?.call(this,o),this.requestUpdate(e,l,s)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??G}static _$Ei(){if(this.hasOwnProperty(C("elementProperties")))return;const e=fe(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(C("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(C("properties"))){const t=this.properties,s=[...me(t),...be(t)];for(const r of s)this.createProperty(r,t[r])}const e=this[Symbol.metadata];if(e!==null){const t=litPropertyMetadata.get(e);if(t!==void 0)for(const[s,r]of t)this.elementProperties.set(s,r)}this._$Eh=new Map;for(const[t,s]of this.elementProperties){const r=this._$Eu(t,s);r!==void 0&&this._$Eh.set(r,t)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){const t=[];if(Array.isArray(e)){const s=new Set(e.flat(1/0).reverse());for(const r of s)t.unshift(V(r))}else e!==void 0&&t.push(V(e));return t}static _$Eu(e,t){const s=t.attribute;return s===!1?void 0:typeof s=="string"?s:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??=new Set).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){const e=new Map,t=this.constructor.elementProperties;for(const s of t.keys())this.hasOwnProperty(s)&&(e.set(s,this[s]),delete this[s]);e.size>0&&(this._$Ep=e)}createRenderRoot(){const e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return he(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,t,s){this._$AK(e,s)}_$ET(e,t){const s=this.constructor.elementProperties.get(e),r=this.constructor._$Eu(e,s);if(r!==void 0&&s.reflect===!0){const a=(s.converter?.toAttribute!==void 0?s.converter:j).toAttribute(t,s.type);this._$Em=e,a==null?this.removeAttribute(r):this.setAttribute(r,a),this._$Em=null}}_$AK(e,t){const s=this.constructor,r=s._$Eh.get(e);if(r!==void 0&&this._$Em!==r){const a=s.getPropertyOptions(r),o=typeof a.converter=="function"?{fromAttribute:a.converter}:a.converter?.fromAttribute!==void 0?a.converter:j;this._$Em=r;const l=o.fromAttribute(t,a.type);this[r]=l??this._$Ej?.get(r)??l,this._$Em=null}}requestUpdate(e,t,s,r=!1,a){if(e!==void 0){const o=this.constructor;if(r===!1&&(a=this[e]),s??=o.getPropertyOptions(e),!((s.hasChanged??B)(a,t)||s.useDefault&&s.reflect&&a===this._$Ej?.get(e)&&!this.hasAttribute(o._$Eu(e,s))))return;this.C(e,t,s)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,t,{useDefault:s,reflect:r,wrapped:a},o){s&&!(this._$Ej??=new Map).has(e)&&(this._$Ej.set(e,o??t??this[e]),a!==!0||o!==void 0)||(this._$AL.has(e)||(this.hasUpdated||s||(t=void 0),this._$AL.set(e,t)),r===!0&&this._$Em!==e&&(this._$Eq??=new Set).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(t){Promise.reject(t)}const e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(const[r,a]of this._$Ep)this[r]=a;this._$Ep=void 0}const s=this.constructor.elementProperties;if(s.size>0)for(const[r,a]of s){const{wrapped:o}=a,l=this[r];o!==!0||this._$AL.has(r)||l===void 0||this.C(r,void 0,a,l)}}let e=!1;const t=this._$AL;try{e=this.shouldUpdate(t),e?(this.willUpdate(t),this._$EO?.forEach(s=>s.hostUpdate?.()),this.update(t)):this._$EM()}catch(s){throw e=!1,this._$EM(),s}e&&this._$AE(t)}willUpdate(e){}_$AE(e){this._$EO?.forEach(t=>t.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&=this._$Eq.forEach(t=>this._$ET(t,this[t])),this._$EM()}updated(e){}firstUpdated(e){}};A.elementStyles=[],A.shadowRootOptions={mode:"open"},A[C("elementProperties")]=new Map,A[C("finalized")]=new Map,$e?.({ReactiveElement:A}),(H.reactiveElementVersions??=[]).push("2.1.2");const J=globalThis,Q=i=>i,R=J.trustedTypes,X=R?R.createPolicy("lit-html",{createHTML:i=>i}):void 0,ae="$lit$",y=`lit$${Math.random().toFixed(9).slice(2)}$`,oe="?"+y,ye=`<${oe}>`,w=document,P=()=>w.createComment(""),O=i=>i===null||typeof i!="object"&&typeof i!="function",F=Array.isArray,_e=i=>F(i)||typeof i?.[Symbol.iterator]=="function",L=`[ 	
\f\r]`,k=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,Y=/-->/g,ee=/>/g,_=RegExp(`>|${L}(?:([^\\s"'>=/]+)(${L}*=${L}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),te=/'/g,se=/"/g,ne=/^(?:script|style|textarea|title)$/i,xe=i=>(e,...t)=>({_$litType$:i,strings:e,values:t}),u=xe(1),S=Symbol.for("lit-noChange"),c=Symbol.for("lit-nothing"),ie=new WeakMap,x=w.createTreeWalker(w,129);function le(i,e){if(!F(i)||!i.hasOwnProperty("raw"))throw Error("invalid template strings array");return X!==void 0?X.createHTML(e):e}const we=(i,e)=>{const t=i.length-1,s=[];let r,a=e===2?"<svg>":e===3?"<math>":"",o=k;for(let l=0;l<t;l++){const n=i[l];let h,p,d=-1,v=0;for(;v<n.length&&(o.lastIndex=v,p=o.exec(n),p!==null);)v=o.lastIndex,o===k?p[1]==="!--"?o=Y:p[1]!==void 0?o=ee:p[2]!==void 0?(ne.test(p[2])&&(r=RegExp("</"+p[2],"g")),o=_):p[3]!==void 0&&(o=_):o===_?p[0]===">"?(o=r??k,d=-1):p[1]===void 0?d=-2:(d=o.lastIndex-p[2].length,h=p[1],o=p[3]===void 0?_:p[3]==='"'?se:te):o===se||o===te?o=_:o===Y||o===ee?o=k:(o=_,r=void 0);const $=o===_&&i[l+1].startsWith("/>")?" ":"";a+=o===k?n+ye:d>=0?(s.push(h),n.slice(0,d)+ae+n.slice(d)+y+$):n+y+(d===-2?l:$)}return[le(i,a+(i[t]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),s]};class D{constructor({strings:e,_$litType$:t},s){let r;this.parts=[];let a=0,o=0;const l=e.length-1,n=this.parts,[h,p]=we(e,t);if(this.el=D.createElement(h,s),x.currentNode=this.el.content,t===2||t===3){const d=this.el.content.firstChild;d.replaceWith(...d.childNodes)}for(;(r=x.nextNode())!==null&&n.length<l;){if(r.nodeType===1){if(r.hasAttributes())for(const d of r.getAttributeNames())if(d.endsWith(ae)){const v=p[o++],$=r.getAttribute(d).split(y),U=/([.?@])?(.*)/.exec(v);n.push({type:1,index:a,name:U[2],strings:$,ctor:U[1]==="."?Se:U[1]==="?"?Ee:U[1]==="@"?ke:q}),r.removeAttribute(d)}else d.startsWith(y)&&(n.push({type:6,index:a}),r.removeAttribute(d));if(ne.test(r.tagName)){const d=r.textContent.split(y),v=d.length-1;if(v>0){r.textContent=R?R.emptyScript:"";for(let $=0;$<v;$++)r.append(d[$],P()),x.nextNode(),n.push({type:2,index:++a});r.append(d[v],P())}}}else if(r.nodeType===8)if(r.data===oe)n.push({type:2,index:a});else{let d=-1;for(;(d=r.data.indexOf(y,d+1))!==-1;)n.push({type:7,index:a}),d+=y.length-1}a++}}static createElement(e,t){const s=w.createElement("template");return s.innerHTML=e,s}}function E(i,e,t=i,s){if(e===S)return e;let r=s!==void 0?t._$Co?.[s]:t._$Cl;const a=O(e)?void 0:e._$litDirective$;return r?.constructor!==a&&(r?._$AO?.(!1),a===void 0?r=void 0:(r=new a(i),r._$AT(i,t,s)),s!==void 0?(t._$Co??=[])[s]=r:t._$Cl=r),r!==void 0&&(e=E(i,r._$AS(i,e.values),r,s)),e}class Ae{constructor(e,t){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=t}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){const{el:{content:t},parts:s}=this._$AD,r=(e?.creationScope??w).importNode(t,!0);x.currentNode=r;let a=x.nextNode(),o=0,l=0,n=s[0];for(;n!==void 0;){if(o===n.index){let h;n.type===2?h=new N(a,a.nextSibling,this,e):n.type===1?h=new n.ctor(a,n.name,n.strings,this,e):n.type===6&&(h=new Ce(a,this,e)),this._$AV.push(h),n=s[++l]}o!==n?.index&&(a=x.nextNode(),o++)}return x.currentNode=w,r}p(e){let t=0;for(const s of this._$AV)s!==void 0&&(s.strings!==void 0?(s._$AI(e,s,t),t+=s.strings.length-2):s._$AI(e[t])),t++}}class N{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,t,s,r){this.type=2,this._$AH=c,this._$AN=void 0,this._$AA=e,this._$AB=t,this._$AM=s,this.options=r,this._$Cv=r?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode;const t=this._$AM;return t!==void 0&&e?.nodeType===11&&(e=t.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,t=this){e=E(this,e,t),O(e)?e===c||e==null||e===""?(this._$AH!==c&&this._$AR(),this._$AH=c):e!==this._$AH&&e!==S&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):_e(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==c&&O(this._$AH)?this._$AA.nextSibling.data=e:this.T(w.createTextNode(e)),this._$AH=e}$(e){const{values:t,_$litType$:s}=e,r=typeof s=="number"?this._$AC(e):(s.el===void 0&&(s.el=D.createElement(le(s.h,s.h[0]),this.options)),s);if(this._$AH?._$AD===r)this._$AH.p(t);else{const a=new Ae(r,this),o=a.u(this.options);a.p(t),this.T(o),this._$AH=a}}_$AC(e){let t=ie.get(e.strings);return t===void 0&&ie.set(e.strings,t=new D(e)),t}k(e){F(this._$AH)||(this._$AH=[],this._$AR());const t=this._$AH;let s,r=0;for(const a of e)r===t.length?t.push(s=new N(this.O(P()),this.O(P()),this,this.options)):s=t[r],s._$AI(a),r++;r<t.length&&(this._$AR(s&&s._$AB.nextSibling,r),t.length=r)}_$AR(e=this._$AA.nextSibling,t){for(this._$AP?.(!1,!0,t);e!==this._$AB;){const s=Q(e).nextSibling;Q(e).remove(),e=s}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}}class q{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,t,s,r,a){this.type=1,this._$AH=c,this._$AN=void 0,this.element=e,this.name=t,this._$AM=r,this.options=a,s.length>2||s[0]!==""||s[1]!==""?(this._$AH=Array(s.length-1).fill(new String),this.strings=s):this._$AH=c}_$AI(e,t=this,s,r){const a=this.strings;let o=!1;if(a===void 0)e=E(this,e,t,0),o=!O(e)||e!==this._$AH&&e!==S,o&&(this._$AH=e);else{const l=e;let n,h;for(e=a[0],n=0;n<a.length-1;n++)h=E(this,l[s+n],t,n),h===S&&(h=this._$AH[n]),o||=!O(h)||h!==this._$AH[n],h===c?e=c:e!==c&&(e+=(h??"")+a[n+1]),this._$AH[n]=h}o&&!r&&this.j(e)}j(e){e===c?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}}class Se extends q{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===c?void 0:e}}class Ee extends q{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==c)}}class ke extends q{constructor(e,t,s,r,a){super(e,t,s,r,a),this.type=5}_$AI(e,t=this){if((e=E(this,e,t,0)??c)===S)return;const s=this._$AH,r=e===c&&s!==c||e.capture!==s.capture||e.once!==s.once||e.passive!==s.passive,a=e!==c&&(s===c||r);r&&this.element.removeEventListener(this.name,this,s),a&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}}class Ce{constructor(e,t,s){this.element=e,this.type=6,this._$AN=void 0,this._$AM=t,this.options=s}get _$AU(){return this._$AM._$AU}_$AI(e){E(this,e)}}const Te=J.litHtmlPolyfillSupport;Te?.(D,N),(J.litHtmlVersions??=[]).push("3.3.3");const Pe=(i,e,t)=>{const s=t?.renderBefore??e;let r=s._$litPart$;if(r===void 0){const a=t?.renderBefore??null;s._$litPart$=r=new N(e.insertBefore(P(),a),a,void 0,t??{})}return r._$AI(i),r};const W=globalThis;class T extends A{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){const e=super.createRenderRoot();return this.renderOptions.renderBefore??=e.firstChild,e}update(e){const t=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=Pe(t,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return S}}T._$litElement$=!0,T.finalized=!0,W.litElementHydrateSupport?.({LitElement:T});const Oe=W.litElementPolyfillSupport;Oe?.({LitElement:T});(W.litElementVersions??=[]).push("4.2.2");const De=i=>(e,t)=>{t!==void 0?t.addInitializer(()=>{customElements.define(i,e)}):customElements.define(i,e)};const Ne={attribute:!0,type:String,converter:j,reflect:!1,hasChanged:B},Ue=(i=Ne,e,t)=>{const{kind:s,metadata:r}=t;let a=globalThis.litPropertyMetadata.get(r);if(a===void 0&&globalThis.litPropertyMetadata.set(r,a=new Map),s==="setter"&&((i=Object.create(i)).wrapped=!0),a.set(t.name,i),s==="accessor"){const{name:o}=t;return{set(l){const n=e.get.call(this);e.set.call(this,l),this.requestUpdate(o,n,i,!0,l)},init(l){return l!==void 0&&this.C(o,void 0,i,l),l}}}if(s==="setter"){const{name:o}=t;return function(l){const n=this[o];e.call(this,l),this.requestUpdate(o,n,i,!0,l)}}throw Error("Unsupported decorator location: "+s)};function Me(i){return(e,t)=>typeof t=="object"?Ue(i,e,t):((s,r,a)=>{const o=r.hasOwnProperty(a);return r.constructor.createProperty(a,s),o?Object.getOwnPropertyDescriptor(r,a):void 0})(i,e,t)}function f(i){return Me({...i,state:!0,attribute:!1})}async function b(i,e){const t=await fetch(i,{...e,headers:{...e?.body?{"content-type":"application/json"}:{},...e?.headers}});if(!t.ok){const s=await t.json().catch(()=>({error:t.statusText}));throw new Error(s.error||t.statusText)}return t.status===204?void 0:t.json()}var je=Object.defineProperty,Re=Object.getOwnPropertyDescriptor,m=(i,e,t,s)=>{for(var r=s>1?void 0:s?Re(e,t):e,a=i.length-1,o;a>=0;a--)(o=i[a])&&(r=(s?o(e,t,r):o(r))||r);return s&&r&&je(e,t,r),r};let g=class extends T{constructor(){super(...arguments),this.targets=[],this.channels=[],this.alerts=[],this.secrets=[],this.error="",this.live=!1,this.saving=!1,this.channelKind="webhook",this.joinCommand=""}connectedCallback(){super.connectedCallback(),this.refresh(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}disconnectedCallback(){this.events?.close(),super.disconnectedCallback()}async refresh(){try{[this.targets,this.channels,this.alerts,this.secrets]=await Promise.all([b("/api/v1/targets"),b("/api/v1/channels"),b("/api/v1/alerts"),b("/api/v1/secrets")]),this.error=""}catch(i){this.error=i instanceof Error?i.message:String(i)}}openTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(i){this.selected=i,this.updateComplete.then(()=>this.renderRoot.querySelector("#detail-dialog")?.showModal())}closeDetailDialog(){this.renderRoot.querySelector("#detail-dialog")?.close(),this.selected=void 0}showDialog(i){this.renderRoot.querySelector(`#${i}`)?.showModal()}closeDialog(i){this.renderRoot.querySelector(`#${i}`)?.close()}async createTarget(i){i.preventDefault();const e=i.currentTarget,t=new FormData(e),s={name:String(t.get("name")),url:String(t.get("url")),method:String(t.get("method")),accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),headers:{},body:null,body_contains:null,skip_tls_verification:!1,notification_channel_ids:[]};this.saving=!0;try{await b("/api/v1/targets",{method:"POST",body:JSON.stringify(s)}),e.reset(),this.closeTargetDialog(),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async updateTarget(i){if(i.preventDefault(),!this.selected)return;const e=i.currentTarget,t=new FormData(e),s=String(t.get("statuses")).split(",").map(a=>{const[o,l]=a.trim().split("-").map(Number);return{start:o,end:l||o}}),r={name:String(t.get("name")),url:String(t.get("url")),method:String(t.get("method")),accepted_statuses:s,follow_redirects:t.get("follow_redirects")==="on",max_redirects:Number(t.get("max_redirects")),interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([a,o])=>[a,o.kind==="literal"?o.value:{secret_id:o.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null,body_contains:String(t.get("body_contains"))||null,skip_tls_verification:t.get("skip_tls_verification")==="on",notification_channel_ids:this.selected.notification_channel_ids};this.saving=!0;try{await b(`/api/v1/targets/${this.selected.id}`,{method:"PUT",body:JSON.stringify(r)}),this.closeDetailDialog(),await this.refresh()}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Delete this target and its history?"))){this.saving=!0;try{await b(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async createSecret(i){i.preventDefault();const e=i.currentTarget,t=new FormData(e);this.saving=!0;try{await b("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:t.get("name"),value:t.get("value")})}),e.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async createChannel(i){i.preventDefault();const e=i.currentTarget,t=new FormData(e),s=this.channelKind==="telegram"?{type:"telegram",name:t.get("name"),bot_token:t.get("bot_token"),chat_id:t.get("chat_id")}:{type:"webhook",name:t.get("name"),url:t.get("url"),headers:{}};this.saving=!0;try{await b("/api/v1/channels",{method:"POST",body:JSON.stringify(s)}),e.reset(),this.channelKind="webhook",this.closeDialog("channel-dialog"),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async createJoinLink(){this.saving=!0;try{const i=await b("/api/v1/join-links",{method:"POST",body:JSON.stringify({expires_in_seconds:600})});this.joinCommand=`upgrid --join '${i.url}'`,this.showDialog("join-dialog")}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}render(){const i=this.targets.filter(s=>s.availability==="up").length,e=this.targets.filter(s=>s.availability==="down").length,t=this.alerts.filter(s=>s.delivery==="pending").length;return u`
      <main class="shell">
        <header>
          <div class="brand">
            <img src="/favicon.svg" alt="" />
            <div><strong>UpGrid</strong><span>Distributed service monitoring</span></div>
          </div>
          <nav aria-label="Primary">
            <a class="active" href="#overview">Overview</a>
            <a href="#targets">Targets</a>
            <a href="#alerts">Alerts</a>
            <a href="#cluster">Cluster</a>
          </nav>
          <div class="actions">
            <button class="button secondary" @click=${this.createJoinLink} ?disabled=${this.saving}>Add node</button>
            <div class="live"><i class="dot ${this.live?"on":""}"></i>${this.live?"live":"connecting"}</div>
          </div>
        </header>
        <section class="heading" id="overview">
          <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
          <button class="button" @click=${this.openTargetDialog}>Add target</button>
        </section>
        ${this.error?u`<div class="notice" role="alert">${this.error}</div>`:c}
        <section class="summary" aria-label="Target summary">
          <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
          <div class="metric"><span>Up</span><strong>${i}</strong></div>
          <div class="metric"><span>Down</span><strong>${e}</strong></div>
          <div class="metric"><span>Pending alerts</span><strong>${t}</strong></div>
        </section>
        <section class="panel" id="targets">
          <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
          ${this.targets.length?this.targets.map(s=>this.renderTarget(s)):u`<div class="empty">No targets yet. Add the first one to begin monitoring.</div>`}
        </section>
        <section class="resources" aria-label="Notification configuration">
          <section class="panel">
            <div class="panel-head"><h2>Notification channels</h2><button class="button secondary" @click=${()=>this.showDialog("channel-dialog")}>Add channel</button></div>
            ${this.channels.length?this.channels.map(s=>u`<div class="resource"><div><strong>${s.name}</strong><code>${s.destination}</code></div><span class="badge">${s.kind}</span></div>`):u`<div class="empty">No notification channels.</div>`}
          </section>
          <section class="panel">
            <div class="panel-head"><h2>Secrets</h2><button class="button secondary" @click=${()=>this.showDialog("secret-dialog")}>Add secret</button></div>
            ${this.secrets.length?this.secrets.map(s=>u`<div class="resource"><div><strong>${s.name}</strong><code>${s.id}</code></div><span class="badge">write-only</span></div>`):u`<div class="empty">No reusable Secrets.</div>`}
          </section>
          <section class="panel" id="alerts">
            <div class="panel-head"><h2>Alert history</h2><span class="meta">${this.alerts.length} events</span></div>
            ${this.alerts.length?this.alerts.slice(0,10).map(s=>u`<div class="resource"><div><strong>${s.target_name}</strong><code>${new Date(s.scheduled_at_ms).toLocaleString()}</code></div><span class="badge">${s.kind} · ${s.delivery}</span></div>`):u`<div class="empty">No availability transitions.</div>`}
          </section>
          <section class="panel" id="cluster">
            <div class="panel-head"><h2>Cluster access</h2><button class="button secondary" @click=${this.createJoinLink}>Add node</button></div>
            <div class="empty">Create a single-use, 10-minute Join Link for each new Node.</div>
          </section>
        </section>
      </main>
      <dialog id="target-dialog" aria-labelledby="add-target-title">
        <div class="dialog-head"><h2 id="add-target-title">Add target</h2><p>Start monitoring an HTTP or HTTPS endpoint.</p></div>
        <form @submit=${this.createTarget}>
          <label>Name<input name="name" placeholder="Production API" required /></label>
          <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
          <div class="row">
            <label>Method<input name="method" value="GET" required /></label>
            <label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label>
          </div>
          <div class="row">
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label>
            <label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label>
          </div>
          <div class="dialog-actions">
            <button class="button secondary" type="button" @click=${this.closeTargetDialog}>Cancel</button>
            <button class="button" type="submit" ?disabled=${this.saving}>${this.saving?"Creating…":"Create target"}</button>
          </div>
        </form>
      </dialog>
      ${this.selected?this.renderDetail(this.selected):c}
      <dialog id="secret-dialog" aria-labelledby="secret-title">
        <div class="dialog-head"><h2 id="secret-title">Add secret</h2><p>The plaintext is encrypted before replication and never returned.</p></div>
        <form @submit=${this.createSecret}>
          <label>Name<input name="name" placeholder="Webhook token" required /></label>
          <label>Value<input name="value" type="password" autocomplete="new-password" required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("secret-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create secret</button></div>
        </form>
      </dialog>
      <dialog id="channel-dialog" aria-labelledby="channel-title">
        <div class="dialog-head"><h2 id="channel-title">Add channel</h2><p>Send transitions through Telegram or a generic webhook.</p></div>
        <form @submit=${this.createChannel}>
          <label>Type<select name="type" @change=${s=>this.channelKind=s.target.value}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
          <label>Name<input name="name" placeholder="On-call" required /></label>
          ${this.channelKind==="webhook"?u`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`:u`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("channel-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create channel</button></div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title">
        <div class="dialog-head"><h2 id="join-title">Join a node</h2><p>This command contains Cluster credentials. Keep it private.</p></div>
        <div class="join-command">${this.joinCommand}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" @click=${()=>this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${()=>navigator.clipboard.writeText(this.joinCommand)}>Copy command</button></div>
      </dialog>
    `}renderTarget(i){const e=i.latest_evaluation;return u`
      <button class="target" aria-label=${i.name} @click=${()=>this.openTarget(i)}>
        <i class="state ${i.availability}" aria-label=${i.availability}></i>
        <div>
          <h3>${i.name}</h3>
          <div class="meta">${i.method} · ${i.url} · every ${i.interval_seconds}s</div>
        </div>
        <div class="latency">
          <strong>${e?`${e.latency_ms} ms`:"—"}</strong>
          <span>${e?e.status_code??"network error":"waiting"}</span>
        </div>
      </button>
    `}renderDetail(i){const e=i.accepted_statuses.map(t=>t.start===t.end?t.start:`${t.start}-${t.end}`).join(",");return u`
      <dialog id="detail-dialog" aria-labelledby="target-detail-title">
        <div class="dialog-head"><h2 id="target-detail-title">Target details</h2><p>${i.id}</p></div>
        <form @submit=${this.updateTarget}>
          <label>Name<input name="name" .value=${i.name} required /></label>
          <label>URL<input name="url" type="url" .value=${i.url} required /></label>
          <div class="row">
            <label>Method<input name="method" .value=${i.method} required /></label>
            <label>Expected statuses<input name="statuses" .value=${e} required /></label>
          </div>
          <div class="row">
            <label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(i.interval_seconds)} required /></label>
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(i.timeout_seconds)} required /></label>
          </div>
          <div class="row">
            <label>Failures before Down<input name="failures" type="number" min="1" .value=${String(i.failure_threshold)} required /></label>
            <label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(i.max_redirects)} required /></label>
          </div>
          <label>Body must contain<input name="body_contains" .value=${i.body_contains??""} /></label>
          <div class="row">
            <label class="check"><input name="follow_redirects" type="checkbox" .checked=${i.follow_redirects} />Follow redirects</label>
            <label class="check"><input name="skip_tls_verification" type="checkbox" .checked=${i.skip_tls_verification} />Skip TLS verification</label>
          </div>
          <div class="dialog-actions">
            <button class="button danger" type="button" @click=${this.deleteTarget}>Delete target</button>
            <button class="button secondary" type="button" @click=${this.closeDetailDialog}>Close</button>
            <button class="button" type="submit" ?disabled=${this.saving}>Save changes</button>
          </div>
        </form>
        <section class="history">
          <h3>Evaluation history</h3>
          ${i.history.length?u`<table><thead><tr><th>Time</th><th>Result</th><th>Status</th><th>Latency</th></tr></thead><tbody>${i.history.map(t=>u`<tr><td>${new Date(t.recorded_at_ms).toLocaleString()}</td><td>${t.succeeded?"Up":"Failed"}</td><td>${t.status_code??"—"}</td><td>${t.latency_ms} ms</td></tr>`)}</tbody></table>`:u`<p class="meta">No evaluations recorded yet.</p>`}
        </section>
      </dialog>
    `}};g.styles=ce`
    :host {
      color-scheme: dark;
      --bg: #090d0c;
      --panel: #111715;
      --panel-2: #151d1a;
      --line: #27322e;
      --muted: #8fa099;
      --text: #edf7f2;
      --green: #58e29c;
      --red: #ff7575;
      --amber: #f2c264;
      display: block;
      min-height: 100vh;
      background:
        radial-gradient(circle at 12% -5%, #18392d 0, transparent 30%),
        linear-gradient(145deg, #090d0c 0%, #0c1210 55%, #09100d 100%);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
    }
    * { box-sizing: border-box; }
    button, input, select { font: inherit; }
    .shell { max-width: 1200px; margin: auto; padding: 28px 24px 72px; }
    header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 34px; }
    .brand, .actions, .live, nav { display: flex; align-items: center; }
    .brand { gap: 13px; }
    .brand img { width: 42px; height: 42px; filter: drop-shadow(0 0 18px #40d89035); }
    .brand strong { display: block; font-size: 19px; letter-spacing: .02em; }
    .brand span, .live, .eyebrow, .meta { color: var(--muted); font-size: 12px; }
    nav { gap: 4px; padding: 4px; border: 1px solid var(--line); border-radius: 11px; background: #0d1210aa; }
    nav a { color: var(--muted); padding: 7px 11px; text-decoration: none; border-radius: 7px; }
    nav a.active { color: var(--text); background: #202b27; }
    .actions { gap: 12px; }
    .live { gap: 7px; }
    .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--amber); }
    .dot.on { background: var(--green); box-shadow: 0 0 10px var(--green); }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 18px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { border: 1px solid #3e765a; border-radius: 9px; background: #1c4a35; color: #e8fff2; padding: 9px 13px; cursor: pointer; }
    .button:hover { border-color: #62b988; }
    .button:disabled { cursor: wait; opacity: .65; }
    .summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
    .metric, .panel { border: 1px solid var(--line); background: #111715dc; box-shadow: 0 16px 48px #0002; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .panel { border-radius: 16px; overflow: hidden; }
    .resources { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-top: 18px; }
    .resource { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 13px 20px; border-bottom: 1px solid #202925; }
    .resource:last-child { border-bottom: 0; }
    .resource strong { display: block; font-size: 13px; }
    .resource code { color: var(--muted); font-size: 11px; }
    .badge { border: 1px solid #3c554a; border-radius: 999px; color: #a7c3b7; padding: 2px 7px; font-size: 10px; text-transform: uppercase; }
    .panel-head { display: flex; align-items: center; justify-content: space-between; padding: 17px 20px; border-bottom: 1px solid var(--line); }
    .panel-head h2 { margin: 0; font-size: 14px; }
    .target { width: 100%; display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 17px 20px; border: 0; border-bottom: 1px solid #202925; background: transparent; color: var(--text); text-align: left; cursor: pointer; }
    .target:hover { background: #17201c; }
    .target:last-child { border-bottom: 0; }
    .state { width: 10px; height: 10px; border-radius: 50%; background: var(--amber); box-shadow: 0 0 12px currentColor; }
    .state.up { color: var(--green); background: var(--green); }
    .state.down { color: var(--red); background: var(--red); }
    .target h3 { margin: 0 0 3px; font-size: 14px; }
    .meta { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .latency { text-align: right; }
    .latency strong { display: block; font-weight: 500; }
    .latency span { color: var(--muted); font-size: 11px; }
    .empty { padding: 54px 20px; color: var(--muted); text-align: center; }
    .notice { margin: 0 0 16px; border: 1px solid #7b3937; border-radius: 10px; background: #391b1a; color: #ffb3af; padding: 10px 12px; }
    dialog { width: min(580px, calc(100% - 28px)); border: 1px solid var(--line); border-radius: 17px; background: var(--panel); color: var(--text); padding: 0; box-shadow: 0 28px 90px #000b; }
    dialog::backdrop { background: #040706cc; backdrop-filter: blur(5px); }
    .dialog-head { padding: 20px 22px 15px; border-bottom: 1px solid var(--line); }
    .dialog-head h2 { margin: 0; font-size: 18px; }
    .dialog-head p { margin: 4px 0 0; color: var(--muted); }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; letter-spacing: .03em; }
    input, select { width: 100%; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: #0c110f; color: var(--text); padding: 9px 10px; }
    input:focus, select:focus { border-color: #4b936c; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .danger { margin-right: auto; background: transparent; color: #ff9b97; border-color: #633b39; }
    .check { display: flex; align-items: center; gap: 8px; }
    .check input { width: auto; }
    .history { margin: 0 22px 22px; border-top: 1px solid var(--line); padding-top: 18px; }
    .history h3 { margin: 0 0 10px; font-size: 14px; }
    .join-command { margin: 20px 22px; border: 1px solid var(--line); border-radius: 10px; background: #0b110e; color: var(--green); padding: 13px; overflow-wrap: anywhere; font: 12px/1.6 ui-monospace, SFMono-Regular, monospace; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { padding: 7px 5px; border-bottom: 1px solid #202925; text-align: left; }
    th { color: var(--muted); font-weight: 500; }
    @media (max-width: 720px) {
      .shell { padding: 20px 14px 60px; }
      nav { display: none; }
      .summary { grid-template-columns: 1fr 1fr; }
      .resources { grid-template-columns: 1fr; }
      .heading { align-items: flex-start; gap: 16px; }
      .target { grid-template-columns: auto minmax(0, 1fr); }
      .latency { grid-column: 2; text-align: left; }
    }
  `;m([f()],g.prototype,"targets",2);m([f()],g.prototype,"channels",2);m([f()],g.prototype,"alerts",2);m([f()],g.prototype,"secrets",2);m([f()],g.prototype,"error",2);m([f()],g.prototype,"live",2);m([f()],g.prototype,"saving",2);m([f()],g.prototype,"selected",2);m([f()],g.prototype,"channelKind",2);m([f()],g.prototype,"joinCommand",2);g=m([De("upgrid-app")],g);
